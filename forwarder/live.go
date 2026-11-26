package forwarder

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/livemix/common"
	"github.com/alwitt/livemix/db"
	"github.com/apex/log"
)

// LiveStreamSegmentForwarder forward video segments in support of a live streaming
// a video source
type LiveStreamSegmentForwarder interface {
	/*
		Stop stop any background support tasks

			@param ctxt context.Context - execution context
	*/
	Stop(ctxt context.Context) error

	/*
		ForwardSegment process a new segment for forwarding

			@param ctxt context.Context - execution context
			@param segment common.VideoSegmentWithData - segment to process
			@param blocking bool - block call until the forwarding complete
	*/
	ForwardSegment(ctxt context.Context, segment common.VideoSegmentWithData, blocking bool) error

	/*
		ForwardSegmentAndWait process a new segment and block until a terminal, explainable
		decision is available.

			@param ctxt context.Context - execution context
			@param segment common.VideoSegmentWithData - segment to process
			@returns the processing result for this submission
	*/
	ForwardSegmentAndWait(
		ctxt context.Context, segment common.VideoSegmentWithData,
	) (SegmentForwardResult, error)
}

// httpLiveStreamSegmentForwarder HTTP version of LiveStreamSegmentForwarder
type httpLiveStreamSegmentForwarder struct {
	goutils.Component
	dbConns          db.ConnectionManager
	client           SegmentSender
	workers          goutils.TaskProcessor
	wg               sync.WaitGroup
	workerCtxt       context.Context
	workerCtxtCancel context.CancelFunc
	retryCfg         RetryConfig
	tracker          *inflightTracker
}

/*
NewHTTPLiveStreamSegmentForwarder define new HTTP version of LiveStreamSegmentForwarder

	@param parentCtxt context.Context - forwarder's parent execution context
	@param dbConns db.ConnectionManager - DB connection manager
	@param sender SegmentSender - client for forwarding video segments to system control node
	@param maxInFlightSegments int - max number of segment being forwarded at any one time
	@param retryCfg RetryConfig - per-segment processing retry behavior
	@param tpMetrics goutils.TaskProcessorMetricHelper - task processor metrics helper
	@returns new LiveStreamSegmentForwarder
*/
func NewHTTPLiveStreamSegmentForwarder(
	parentCtxt context.Context,
	dbConns db.ConnectionManager,
	sender SegmentSender,
	maxInFlightSegments int,
	retryCfg RetryConfig,
	tpMetrics goutils.TaskProcessorMetricHelper,
) (LiveStreamSegmentForwarder, error) {
	logTags := log.Fields{
		"module":    "forwarder",
		"component": "http-segment-forwarder",
	}

	instance := &httpLiveStreamSegmentForwarder{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		dbConns:  dbConns,
		client:   sender,
		wg:       sync.WaitGroup{},
		retryCfg: normalizeRetryConfig(retryCfg),
		tracker:  newInflightTracker(),
	}

	// Worker context
	instance.workerCtxt, instance.workerCtxtCancel = context.WithCancel(parentCtxt)

	// Support worker
	workerLogsTags := log.Fields{
		"module":     "forwarder",
		"component":  "http-segment-forwarder",
		"sub-module": "support-worker",
	}
	workers, err := goutils.GetNewTaskDemuxProcessorInstance(
		instance.workerCtxt,
		"segment-forwarder-core",
		maxInFlightSegments+1,
		maxInFlightSegments+1,
		workerLogsTags,
		tpMetrics,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to define support worker")
		return nil, err
	}
	instance.workers = workers

	// -----------------------------------------------------------------------------
	// Define support tasks

	if err := workers.AddToTaskExecutionMap(
		reflect.TypeOf(liveSegFwdForwardReq{}), instance.forwardSegment,
	); err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to install task definition")
		return nil, err
	}

	// -----------------------------------------------------------------------------
	// Start the worker

	if err := workers.StartEventLoop(&instance.wg); err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to start the worker threads")
		return nil, err
	}

	return instance, nil
}

// stopDrainTimeout bounds how long Stop waits for in-flight segments to finish before
// the worker context is cancelled and remaining work is explicitly aborted.
const stopDrainTimeout = 10 * time.Second

func (f *httpLiveStreamSegmentForwarder) Stop(ctxt context.Context) error {
	logTags := f.GetLogTagsForContext(ctxt)

	// Reject new submissions and unblock waiters immediately
	f.tracker.beginShutdown()

	// Give in-flight segments a bounded chance to finish with an explicit result
	drainCtxt, drainCancel := context.WithTimeout(ctxt, stopDrainTimeout)
	if err := f.tracker.waitInflight(drainCtxt); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Warn("Timed out waiting for in-flight live segments; cancelling remaining work")
	}
	drainCancel()

	f.workerCtxtCancel()
	if err := f.workers.StopEventLoop(); err != nil {
		return err
	}
	if err := goutils.TimeBoundedWaitGroupWait(ctxt, &f.wg, time.Second*10); err != nil {
		return err
	}

	// Any tasks still queued when the event loop exited are explicitly cancelled
	if aborted := f.tracker.abortInflight(); aborted > 0 {
		log.
			WithFields(logTags).
			WithField("aborted-segments", aborted).
			Warn("Cancelled queued live segments that never started processing")
	}

	// Surface any segments whose processing ultimately failed
	for _, failure := range f.tracker.failedRecords() {
		log.
			WithFields(logTags).
			WithField("segment-id", failure.SegmentID).
			WithField("segment-name", failure.SegmentName).
			WithField("attempts", failure.Attempts).
			WithError(failure.Err).
			Error("Live segment forwarding failed and retries were exhausted")
	}
	return nil
}

// ======================================================================================
// Segment Processing

type liveSegFwdForwardReq struct {
	Segment common.VideoSegmentWithData
	// key identity used for de-duplication
	key string
	// entry single-flight entry this run owns
	entry *inflightSegment
}

func (f *httpLiveStreamSegmentForwarder) ForwardSegment(
	ctxt context.Context, segment common.VideoSegmentWithData, blocking bool,
) error {
	req, err := f.submit(ctxt, segment)
	if err != nil {
		return err
	}
	if !blocking {
		// Non-blocking call: the worker run still owns the lifecycle; this submission's
		// reference is dropped immediately.
		f.tracker.release(req.key)
		return nil
	}
	result, waitErr := f.tracker.waitForResult(ctxt, req.entry)
	f.tracker.release(req.key)
	if waitErr != nil {
		return waitErr
	}
	if result.Status == SegmentStatusFailed {
		return result.Err
	}
	return nil
}

func (f *httpLiveStreamSegmentForwarder) ForwardSegmentAndWait(
	ctxt context.Context, segment common.VideoSegmentWithData,
) (SegmentForwardResult, error) {
	req, err := f.submit(ctxt, segment)
	if err != nil {
		return SegmentForwardResult{}, err
	}
	result, waitErr := f.tracker.waitForResult(ctxt, req.entry)
	f.tracker.release(req.key)
	return result, waitErr
}

// submit reserves the segment through the single-flight tracker and, when this call owns
// the processing run, enqueues it on the worker pool. Concurrent duplicate submissions
// receive the shared entry and never trigger a second outbound upload.
func (f *httpLiveStreamSegmentForwarder) submit(
	ctxt context.Context, segment common.VideoSegmentWithData,
) (liveSegFwdForwardReq, error) {
	logTags := f.GetLogTagsForContext(ctxt)
	key := segmentIdentity(segment)
	base := SegmentForwardResult{SegmentID: segment.ID, SegmentName: segment.Name}

	entry, started, alreadyDone, ok := f.tracker.acquire(key, base)
	if !ok {
		log.
			WithFields(logTags).
			WithField("source-id", segment.SourceID).
			WithField("segment-name", segment.Name).
			Warn("Rejecting live segment submission: forwarder is stopping")
		return liveSegFwdForwardReq{}, errForwarderStopping
	}

	req := liveSegFwdForwardReq{Segment: segment, key: key, entry: entry}
	if !started {
		if alreadyDone {
			// The segment already reached a terminal state; reuse that result without
			// triggering any new outbound request.
			log.
				WithFields(logTags).
				WithField("source-id", segment.SourceID).
				WithField("segment-name", segment.Name).
				WithField("decision", entry.result.Decision.String()).
				Debug("Live segment already processed; reusing terminal result")
			return req, nil
		}
		// Concurrent duplicate: the owner run will produce the single result.
		log.
			WithFields(logTags).
			WithField("source-id", segment.SourceID).
			WithField("segment-name", segment.Name).
			Debug("Live segment already being processed; joining in-flight run")
		return req, nil
	}

	log.
		WithFields(logTags).
		WithField("source-id", segment.SourceID).
		WithField("segment-name", segment.Name).
		Debug("Submitting new segment for processing")

	if err := f.workers.Submit(ctxt, req); err != nil {
		// Submission failed (shutdown / context): make the cancellation explicit instead
		// of leaving any waiter blocked forever.
		f.tracker.finish(key, SegmentForwardResult{
			SegmentID:   segment.ID,
			SegmentName: segment.Name,
			Status:      SegmentStatusCancelled,
			Decision:    SegmentDecisionProcess,
		})
		f.tracker.release(key)
		log.
			WithError(err).
			WithFields(logTags).
			WithField("source-id", segment.SourceID).
			WithField("segment-name", segment.Name).
			Error("Failed to submit new segment for processing")
		return liveSegFwdForwardReq{}, err
	}
	return req, nil
}

func (f *httpLiveStreamSegmentForwarder) forwardSegment(params interface{}) error {
	// Convert params into expected data type
	if segment, ok := params.(liveSegFwdForwardReq); ok {
		return f.handleForwardSegment(segment)
	}
	err := fmt.Errorf("received unexpected call parameters: %s", reflect.TypeOf(params))
	logTags := f.GetLogTagsForContext(f.workerCtxt)
	log.WithError(err).WithFields(logTags).Error("'ForwardSegment' processing failure")
	return err
}

// finalize records the single terminal result for the owner run and releases the
// owner's reference
func (f *httpLiveStreamSegmentForwarder) finalize(
	req liveSegFwdForwardReq, result SegmentForwardResult,
) {
	f.tracker.finish(req.key, result)
	f.tracker.release(req.key)
}

func (f *httpLiveStreamSegmentForwarder) handleForwardSegment(
	segmentWrapper liveSegFwdForwardReq,
) error {
	logTags := f.GetLogTagsForContext(f.workerCtxt)
	segment := segmentWrapper.Segment

	baseResult := SegmentForwardResult{
		SegmentID:   segment.ID,
		SegmentName: segment.Name,
	}

	var lastErr error
	attempts := 0

	for attempt := 1; attempt <= f.retryCfg.MaxAttempts; attempt++ {
		// Stopping workers must not start new outbound work
		if f.tracker.isStopped() {
			result := baseResult
			result.Status = SegmentStatusCancelled
			result.Decision = SegmentDecisionProcess
			result.Attempts = attempts
			f.finalize(segmentWrapper, result)
			return errForwarderStopping
		}
		attempts = attempt

		dbClient := f.dbConns.NewPersistanceManager()
		source, lookupErr := dbClient.GetVideoSource(f.workerCtxt, segment.SourceID)
		dbClient.Close()
		if lookupErr != nil {
			lastErr = lookupErr
			log.
				WithError(lookupErr).
				WithFields(logTags).
				WithField("source-id", segment.SourceID).
				WithField("segment-name", segment.Name).
				WithField("attempt", attempt).
				Warn("Unable to read video source entry; retrying")
			if f.sleepOrCancel(segmentWrapper, baseResult, attempts) {
				return errForwarderStopping
			}
			continue
		}

		// Explicit, explainable skip: source streaming disabled. No outbound request.
		if source.Streaming != 1 {
			log.
				WithFields(logTags).
				WithField("source-id", segment.SourceID).
				WithField("segment-name", segment.Name).
				WithField("decision", SegmentDecisionSkippedStreamingDisabled.String()).
				Info("Skipping live segment: video source streaming is disabled")
			result := baseResult
			result.Status = SegmentStatusSkipped
			result.Decision = SegmentDecisionSkippedStreamingDisabled
			result.Attempts = attempts
			f.finalize(segmentWrapper, result)
			return nil
		}

		// Forward the segment
		log.
			WithFields(logTags).
			WithField("source-id", segment.SourceID).
			WithField("segment-name", segment.Name).
			Debug("Sending out segment")
		if fwdErr := f.client.ForwardSegment(f.workerCtxt, segment); fwdErr != nil {
			lastErr = fwdErr
			log.
				WithError(fwdErr).
				WithFields(logTags).
				WithField("segment-name", segment.Name).
				WithField("attempt", attempt).
				WithField("max-attempts", f.retryCfg.MaxAttempts).
				Warn("Live segment forwarding attempt failed")
			if f.sleepOrCancel(segmentWrapper, baseResult, attempts) {
				return errForwarderStopping
			}
			continue
		}

		result := baseResult
		result.Status = SegmentStatusProcessed
		result.Decision = SegmentDecisionProcess
		result.Attempts = attempts
		f.tracker.clearFailure(segmentWrapper.key)
		f.finalize(segmentWrapper, result)
		log.
			WithFields(logTags).
			WithField("segment-name", segment.Name).
			WithField("attempts", attempts).
			Debug("Live segment processing completed")
		return nil
	}

	// Retries exhausted: record an observable failure instead of pretending success
	result := baseResult
	result.Status = SegmentStatusFailed
	result.Decision = SegmentDecisionProcess
	result.Attempts = attempts
	result.Err = lastErr
	f.finalize(segmentWrapper, result)
	log.
		WithError(lastErr).
		WithFields(logTags).
		WithField("segment-name", segment.Name).
		WithField("attempts", attempts).
		Error("Live segment processing failed and retries were exhausted")
	return lastErr
}

// sleepOrCancel performs the inter-attempt backoff, and finalizes a cancelled result when
// shutdown interrupts the wait. Returns true when the run has been cancelled.
func (f *httpLiveStreamSegmentForwarder) sleepOrCancel(
	req liveSegFwdForwardReq, baseResult SegmentForwardResult, attempts int,
) bool {
	if sleepErr := interruptibleSleep(
		f.workerCtxt, f.tracker.stopCh, f.retryCfg.nextBackoff(attempts),
	); sleepErr != nil {
		result := baseResult
		result.Status = SegmentStatusCancelled
		result.Decision = SegmentDecisionProcess
		result.Attempts = attempts
		f.finalize(req, result)
		return true
	}
	return false
}
