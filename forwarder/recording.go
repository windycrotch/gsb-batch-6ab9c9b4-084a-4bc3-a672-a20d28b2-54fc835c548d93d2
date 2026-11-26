package forwarder

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/livemix/common"
	"github.com/alwitt/livemix/common/ipc"
	"github.com/alwitt/livemix/db"
	"github.com/alwitt/livemix/utils"
	"github.com/apex/log"
)

// RecordingSegmentForwarder forward video segments associated with active recording sessions
type RecordingSegmentForwarder interface {
	/*
		Stop stop any background support tasks

			@param ctxt context.Context - execution context
	*/
	Stop(ctxt context.Context) error

	/*
		ForwardSegment process new segments associated with active video recording sessions

			@param ctxt context.Context - execution context
			@param recordings []string - set of recording session IDs
			@param segments []common.VideoSegmentWithData - new video segments to report
	*/
	ForwardSegment(
		ctxt context.Context, recordings []string, segments []common.VideoSegmentWithData,
	) error

	/*
		FailedSegments return segments whose upload / broadcast retries were exhausted. These
		observable failure records replace the old behavior where a failed upload was silently
		lost or falsely reported as stored.

			@returns snapshot of failed segment records
	*/
	FailedSegments() []FailedSegmentRecord
}

// s3RecordingSegmentForwarder S3 version of RecordingSegmentForwarder
type s3RecordingSegmentForwarder struct {
	goutils.Component
	storageCfg       common.RecordingStorageConfig
	dbConns          db.ConnectionManager
	s3Client         SegmentSender
	broadcastClient  utils.Broadcaster
	txWorkers        goutils.TaskProcessor
	wg               sync.WaitGroup
	workerCtxt       context.Context
	workerCtxtCancel context.CancelFunc
	retryCfg         RetryConfig
	tracker          *inflightTracker
}

/*
NewS3RecordingSegmentForwarder define new S3 version of RecordingSegmentForwarder

	@param parentCtxt context.Context - forwarder's parent execution context
	@param storageCfg common.RecordingStorageConfig - segment storage config
	@param dbConns db.ConnectionManager - DB connection manager for recording session checks
	@param s3Client SegmentSender - S3 segment transport client
	@param broadcastClient utils.Broadcaster - message broadcast client
	@param maxInFlightSegments int - max number of segment being stored at any one time
	@param retryCfg RetryConfig - per-segment processing retry behavior
	@param tpMetrics goutils.TaskProcessorMetricHelper - task processor metrics helper
	@returns new RecordingSegmentForwarder
*/
func NewS3RecordingSegmentForwarder(
	parentCtxt context.Context,
	storageCfg common.RecordingStorageConfig,
	dbConns db.ConnectionManager,
	s3Client SegmentSender,
	broadcastClient utils.Broadcaster,
	maxInFlightSegments int,
	retryCfg RetryConfig,
	tpMetrics goutils.TaskProcessorMetricHelper,
) (RecordingSegmentForwarder, error) {
	logTags := log.Fields{
		"module":    "forwarder",
		"component": "s3-recording-segment-forwarder",
	}

	instance := &s3RecordingSegmentForwarder{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		storageCfg:      storageCfg,
		dbConns:         dbConns,
		s3Client:        s3Client,
		broadcastClient: broadcastClient,
		wg:              sync.WaitGroup{},
		retryCfg:        normalizeRetryConfig(retryCfg),
		tracker:         newInflightTracker(),
	}

	// Worker context
	instance.workerCtxt, instance.workerCtxtCancel = context.WithCancel(parentCtxt)

	// -----------------------------------------------------------------------------
	// Prepare TX workers
	txWorkerLogTags := log.Fields{
		"module":     "forwarder",
		"component":  "s3-recording-segment-forwarder",
		"sub-module": "transmit-worker",
	}
	workers, err := goutils.GetNewTaskDemuxProcessorInstance(
		instance.workerCtxt,
		"segment-forwarder-transmit",
		maxInFlightSegments+1,
		maxInFlightSegments+1,
		txWorkerLogTags,
		tpMetrics,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to define transmission worker")
		return nil, err
	}
	instance.txWorkers = workers

	// Define support tasks
	if err := workers.AddToTaskExecutionMap(
		reflect.TypeOf(recordingSegmentForwardRequest{}), instance.forwardSegment,
	); err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to install task definition")
		return nil, err
	}

	// -----------------------------------------------------------------------------
	// Start the worker

	if err := workers.StartEventLoop(&instance.wg); err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to start the transmission worker threads")
		return nil, err
	}

	return instance, nil
}

func (f *s3RecordingSegmentForwarder) Stop(ctxt context.Context) error {
	logTags := f.GetLogTagsForContext(ctxt)

	// Reject new submissions, unblock waiters, and prevent late broadcasts
	f.tracker.beginShutdown()

	// Give in-flight segments a bounded chance to finish with an explicit result
	drainCtxt, drainCancel := context.WithTimeout(ctxt, stopDrainTimeout)
	if err := f.tracker.waitInflight(drainCtxt); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Warn("Timed out waiting for in-flight recording segments; cancelling remaining work")
	}
	drainCancel()

	f.workerCtxtCancel()
	if err := f.txWorkers.StopEventLoop(); err != nil {
		return err
	}
	if err := goutils.TimeBoundedWaitGroupWait(ctxt, &f.wg, time.Second*10); err != nil {
		return err
	}

	// Any tasks still queued when the event loop exited are explicitly cancelled; this
	// also guarantees no late broadcast is written after Stop.
	if aborted := f.tracker.abortInflight(); aborted > 0 {
		log.
			WithFields(logTags).
			WithField("aborted-segments", aborted).
			Warn("Cancelled queued recording segments that never started processing")
	}

	// Surface every segment whose retries were exhausted so failures stay observable
	for _, failure := range f.tracker.failedRecords() {
		log.
			WithFields(logTags).
			WithField("segment-id", failure.SegmentID).
			WithField("segment-name", failure.SegmentName).
			WithField("storage-uri", failure.URI).
			WithField("attempts", failure.Attempts).
			WithError(failure.Err).
			Error("Recording segment upload failed and retries were exhausted")
	}
	return nil
}

func (f *s3RecordingSegmentForwarder) FailedSegments() []FailedSegmentRecord {
	return f.tracker.failedRecords()
}

// ======================================================================================
// Segment Processing

type recordingSegmentForwardRequest struct {
	recording []string
	segment   common.VideoSegmentWithData
	// key identity used for de-duplication
	key string
}

func (f *s3RecordingSegmentForwarder) ForwardSegment(
	ctxt context.Context, recordings []string, segments []common.VideoSegmentWithData,
) error {
	logTags := f.GetLogTagsForContext(ctxt)

	if f.tracker.isStopped() {
		log.WithFields(logTags).Warn("Rejecting recording segment submission: forwarder is stopping")
		return errForwarderStopping
	}

	// Process each segment separately
	requests := []recordingSegmentForwardRequest{}
	for _, oneSegment := range segments {
		// Define segment storage URI. This tells the sender where to place the object
		oneSegment.URI = fmt.Sprintf(
			"s3://%s/%s/%s/%s",
			f.storageCfg.StorageBucket,
			f.storageCfg.StorageObjectPrefix,
			oneSegment.SourceID,
			oneSegment.Name,
		)
		log.
			WithFields(logTags).
			WithField("source-id", oneSegment.SourceID).
			WithField("segment", oneSegment.Name).
			WithField("storage-uri", oneSegment.URI).
			Debug("Newly defined recording segment storage URI")

		key := segmentIdentity(oneSegment)
		base := SegmentForwardResult{
			SegmentID:   oneSegment.ID,
			SegmentName: oneSegment.Name,
			URI:         oneSegment.URI,
		}
		_, started, alreadyDone, ok := f.tracker.acquire(key, base)
		if !ok {
			log.
				WithFields(logTags).
				WithField("segment", oneSegment.Name).
				Warn("Rejecting recording segment submission: forwarder is stopping")
			return errForwarderStopping
		}
		if !started {
			// Concurrent duplicate or already finalized: collapse onto the single owner
			// run so that only one upload and one broadcast ever happen for this segment.
			log.
				WithFields(logTags).
				WithField("segment", oneSegment.Name).
				WithField("already-finalized", alreadyDone).
				Debug("Recording segment submission joined existing processing state")
			f.tracker.release(key)
			continue
		}
		requests = append(requests, recordingSegmentForwardRequest{
			recording: recordings,
			segment:   oneSegment,
			key:       key,
		})
	}

	log.
		WithFields(logTags).
		WithField("segments-to-record", len(requests)).
		Debug("Submitting new recording segment forward requests")

	for _, request := range requests {
		if err := f.txWorkers.Submit(ctxt, request); err != nil {
			log.
				WithError(err).
				WithFields(logTags).
				WithField("source-id", request.segment.SourceID).
				WithField("segment", request.segment.Name).
				Error("Failed to submit a new recording segment forward request")
			// Make the cancellation explicit instead of leaving waiters blocked
			f.tracker.finish(request.key, SegmentForwardResult{
				SegmentID:   request.segment.ID,
				SegmentName: request.segment.Name,
				URI:         request.segment.URI,
				Status:      SegmentStatusCancelled,
				Decision:    SegmentDecisionProcess,
			})
			f.tracker.release(request.key)
		}
	}

	log.
		WithFields(logTags).
		WithField("segments-to-record", len(requests)).
		Debug("Processed all new recording segment forward requests")

	return nil
}

func (f *s3RecordingSegmentForwarder) forwardSegment(params interface{}) error {
	if request, ok := params.(recordingSegmentForwardRequest); ok {
		return f.handleForwardSegment(request)
	}
	err := fmt.Errorf("received unexpected call parameters: %s", reflect.TypeOf(params))
	logTags := f.GetLogTagsForContext(f.workerCtxt)
	log.WithError(err).WithFields(logTags).Error("'ForwardSegment' processing failure")
	return err
}

// finalize records the single terminal result for the owner run and releases the
// owner's reference
func (f *s3RecordingSegmentForwarder) finalize(
	req recordingSegmentForwardRequest, result SegmentForwardResult,
) {
	f.tracker.finish(req.key, result)
	f.tracker.release(req.key)
}

func (f *s3RecordingSegmentForwarder) handleForwardSegment(
	param recordingSegmentForwardRequest,
) error {
	logTags := f.GetLogTagsForContext(f.workerCtxt)

	baseResult := SegmentForwardResult{
		SegmentID:   param.segment.ID,
		SegmentName: param.segment.Name,
		URI:         param.segment.URI,
	}

	var lastErr error
	attempts := 0

	for attempt := 1; attempt <= f.retryCfg.MaxAttempts; attempt++ {
		// Do not start / continue outbound work once shutdown began
		if f.tracker.isStopped() {
			result := baseResult
			result.Status = SegmentStatusCancelled
			result.Decision = SegmentDecisionProcess
			result.Attempts = attempts
			f.finalize(param, result)
			return errForwarderStopping
		}
		attempts = attempt

		// Re-verify at processing time that at least one attached recording session is
		// still active. Segments arriving after the session ended are not counted toward
		// it and are never broadcast.
		activeRecordings, verifyErr := f.activeRecordings(param.recording)
		if verifyErr != nil {
			lastErr = verifyErr
			log.
				WithError(verifyErr).
				WithFields(logTags).
				WithField("segment", param.segment.Name).
				WithField("attempt", attempt).
				Warn("Unable to verify recording session state; retrying")
			if f.sleepOrCancel(param, baseResult, attempts) {
				return errForwarderStopping
			}
			continue
		}
		if len(activeRecordings) == 0 {
			log.
				WithFields(logTags).
				WithField("segment", param.segment.Name).
				WithField("decision", SegmentDecisionSkippedNoActiveRecording.String()).
				Info("Skipping recording segment: no attached recording session is active")
			result := baseResult
			result.Status = SegmentStatusSkipped
			result.Decision = SegmentDecisionSkippedNoActiveRecording
			result.Attempts = attempts
			f.finalize(param, result)
			return nil
		}

		// Upload the segment when it has not been stored before
		if param.segment.Uploaded == nil {
			log.
				WithFields(logTags).
				WithField("source-id", param.segment.SourceID).
				WithField("segment", param.segment.Name).
				WithField("storage-uri", param.segment.URI).
				Debug("Storing recording segment")

			if err := f.s3Client.ForwardSegment(f.workerCtxt, param.segment); err != nil {
				lastErr = err
				log.
					WithError(err).
					WithFields(logTags).
					WithField("source-id", param.segment.SourceID).
					WithField("segment", param.segment.Name).
					WithField("storage-uri", param.segment.URI).
					WithField("attempt", attempt).
					WithField("max-attempts", f.retryCfg.MaxAttempts).
					Warn("Recording segment upload attempt failed")
				if f.sleepOrCancel(param, baseResult, attempts) {
					return errForwarderStopping
				}
				continue
			}

			log.
				WithFields(logTags).
				WithField("source-id", param.segment.SourceID).
				WithField("segment", param.segment.Name).
				WithField("storage-uri", param.segment.URI).
				Debug("Recording segment stored")
		}

		// Broadcast only after the object is known to exist, and guard against late
		// broadcasts during shutdown
		if f.tracker.isStopped() {
			result := baseResult
			result.Status = SegmentStatusCancelled
			result.Decision = SegmentDecisionProcess
			result.Attempts = attempts
			f.finalize(param, result)
			return errForwarderStopping
		}

		report := ipc.NewRecordingSegmentReport(
			activeRecordings, []common.VideoSegment{param.segment.VideoSegment},
		)
		if err := f.broadcastClient.Broadcast(f.workerCtxt, &report); err != nil {
			lastErr = err
			log.
				WithError(err).
				WithFields(logTags).
				WithField("source-id", param.segment.SourceID).
				WithField("segment", param.segment.Name).
				WithField("storage-uri", param.segment.URI).
				WithField("attempt", attempt).
				WithField("max-attempts", f.retryCfg.MaxAttempts).
				Warn("Recording segment broadcast attempt failed")
			// The object exists; a retry must not re-upload it, so mark the segment as
			// uploaded before retrying the broadcast.
			uploaded := 1
			param.segment.Uploaded = &uploaded
			if f.sleepOrCancel(param, baseResult, attempts) {
				return errForwarderStopping
			}
			continue
		}

		result := baseResult
		result.Status = SegmentStatusProcessed
		result.Decision = SegmentDecisionProcess
		result.Attempts = attempts
		f.tracker.clearFailure(param.key)
		f.finalize(param, result)
		log.
			WithFields(logTags).
			WithField("segment", param.segment.Name).
			WithField("attempts", attempts).
			Debug("Recording segment processing completed")
		return nil
	}

	// Retries exhausted: retain an observable failure record instead of pretending success
	result := baseResult
	result.Status = SegmentStatusFailed
	result.Decision = SegmentDecisionProcess
	result.Attempts = attempts
	result.Err = lastErr
	f.finalize(param, result)
	log.
		WithError(lastErr).
		WithFields(logTags).
		WithField("segment", param.segment.Name).
		WithField("storage-uri", param.segment.URI).
		WithField("attempts", attempts).
		Error("Recording segment processing failed and retries were exhausted")
	return lastErr
}

// activeRecordings filters the requested recording session IDs down to those still
// active in persistence at processing time
func (f *s3RecordingSegmentForwarder) activeRecordings(recordingIDs []string) ([]string, error) {
	dbClient := f.dbConns.NewPersistanceManager()
	defer dbClient.Close()

	active := []string{}
	for _, recordingID := range recordingIDs {
		session, err := dbClient.GetRecordingSession(f.workerCtxt, recordingID)
		if err != nil {
			return nil, err
		}
		if session.Active == 1 {
			active = append(active, recordingID)
		}
	}
	return active, nil
}

// sleepOrCancel performs the inter-attempt backoff, finalizing a cancelled result when
// shutdown interrupts the wait. Returns true when the run has been cancelled.
func (f *s3RecordingSegmentForwarder) sleepOrCancel(
	req recordingSegmentForwardRequest, baseResult SegmentForwardResult, attempts int,
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
