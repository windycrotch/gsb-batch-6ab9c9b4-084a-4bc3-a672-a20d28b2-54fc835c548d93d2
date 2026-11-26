package forwarder_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alwitt/livemix/common"
	"github.com/alwitt/livemix/forwarder"
	"github.com/alwitt/livemix/hls"
	"github.com/alwitt/livemix/mocks"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func fastRetryCfg() forwarder.RetryConfig {
	return forwarder.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	}
}

func newTestLiveSegment(sourceID string) common.VideoSegmentWithData {
	return common.VideoSegmentWithData{
		VideoSegment: common.VideoSegment{
			ID:       uuid.NewString(),
			SourceID: sourceID,
			Segment:  hls.Segment{Name: uuid.NewString() + ".ts"},
		},
		Content: []byte(uuid.NewString()),
	}
}

func TestHTTPLiveStreamForwarder(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()

	mockSQL := mocks.NewConnectionManager(t)
	mockDB := mocks.NewPersistenceManager(t)
	mockSQL.On("NewPersistanceManager").Return(mockDB).Maybe()
	mockDB.On("Close").Return().Maybe()
	mockSender := mocks.NewSegmentSender(t)

	uut, err := forwarder.NewHTTPLiveStreamSegmentForwarder(
		utCtxt, mockSQL, mockSender, 2, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	// ------------------------------------------------------------------------------------
	// Case 0: source not streaming -> explicit skip, no outbound request

	{
		testSource := common.VideoSource{ID: uuid.NewString(), Streaming: -1}
		newSegment := newTestLiveSegment(testSource.ID)

		mockDB.On(
			"GetVideoSource",
			mock.AnythingOfType("*context.cancelCtx"),
			testSource.ID,
		).Return(testSource, nil).Once()

		// Blocking call must return nil (skip, not error) without any sender call
		assert.Nil(uut.ForwardSegment(utCtxt, newSegment, true))

		// The explainable result must identify the skip decision explicitly
		result, resErr := uut.ForwardSegmentAndWait(utCtxt, newSegment)
		assert.Nil(resErr)
		assert.Equal(forwarder.SegmentStatusSkipped, result.Status)
		assert.Equal(forwarder.SegmentDecisionSkippedStreamingDisabled, result.Decision)
	}

	// ------------------------------------------------------------------------------------
	// Case 1: source is streaming -> forwarded once

	{
		testSource := common.VideoSource{ID: uuid.NewString(), Streaming: 1}
		newSegment := newTestLiveSegment(testSource.ID)

		mockDB.On(
			"GetVideoSource",
			mock.AnythingOfType("*context.cancelCtx"),
			testSource.ID,
		).Return(testSource, nil).Once()
		mockSender.On(
			"ForwardSegment",
			mock.AnythingOfType("*context.cancelCtx"),
			mock.AnythingOfType("common.VideoSegmentWithData"),
		).Run(func(args mock.Arguments) {
			segment, ok := args.Get(1).(common.VideoSegmentWithData)
			assert.True(ok)
			assert.Equal(testSource.ID, segment.SourceID)
			assert.Equal(newSegment.Name, segment.Name)
			assert.Equal(newSegment.Content, segment.Content)
		}).Return(nil).Once()

		result, resErr := uut.ForwardSegmentAndWait(utCtxt, newSegment)
		assert.Nil(resErr)
		assert.Equal(forwarder.SegmentStatusProcessed, result.Status)
		assert.Equal(forwarder.SegmentDecisionProcess, result.Decision)
		assert.Equal(1, result.Attempts)
	}

	// Clean up
	assert.Nil(uut.Stop(utCtxt))
}

func TestHTTPLiveStreamForwarderSkipMakesNoOutboundRequest(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()

	mockSQL := mocks.NewConnectionManager(t)
	mockDB := mocks.NewPersistenceManager(t)
	mockSQL.On("NewPersistanceManager").Return(mockDB).Maybe()
	mockDB.On("Close").Return().Maybe()
	// Sender has no expectations; any call would fail the test on cleanup
	mockSender := mocks.NewSegmentSender(t)

	uut, err := forwarder.NewHTTPLiveStreamSegmentForwarder(
		utCtxt, mockSQL, mockSender, 2, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	testSource := common.VideoSource{ID: uuid.NewString(), Streaming: -1}
	newSegment := newTestLiveSegment(testSource.ID)

	mockDB.On(
		"GetVideoSource",
		mock.AnythingOfType("*context.cancelCtx"),
		testSource.ID,
	).Return(testSource, nil)

	for i := 0; i < 5; i++ {
		assert.Nil(uut.ForwardSegment(utCtxt, newSegment, true))
	}

	assert.Nil(uut.Stop(utCtxt))
}

func TestHTTPLiveStreamForwarderRetryThenSuccess(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()

	mockSQL := mocks.NewConnectionManager(t)
	mockDB := mocks.NewPersistenceManager(t)
	mockSQL.On("NewPersistanceManager").Return(mockDB).Maybe()
	mockDB.On("Close").Return().Maybe()
	mockSender := mocks.NewSegmentSender(t)

	retryCfg := fastRetryCfg()
	uut, err := forwarder.NewHTTPLiveStreamSegmentForwarder(
		utCtxt, mockSQL, mockSender, 2, retryCfg, nil,
	)
	assert.Nil(err)

	testSource := common.VideoSource{ID: uuid.NewString(), Streaming: 1}
	newSegment := newTestLiveSegment(testSource.ID)
	mockDB.On(
		"GetVideoSource",
		mock.AnythingOfType("*context.cancelCtx"),
		testSource.ID,
	).Return(testSource, nil)

	var attempts int32
	mockSender.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Run(func(args mock.Arguments) {
		atomic.AddInt32(&attempts, 1)
	}).Return(func(context.Context, common.VideoSegmentWithData) error {
		if atomic.LoadInt32(&attempts) < 3 {
			return errors.New("transient network failure")
		}
		return nil
	}).Times(3)

	result, resErr := uut.ForwardSegmentAndWait(utCtxt, newSegment)
	assert.Nil(resErr)
	assert.Equal(forwarder.SegmentStatusProcessed, result.Status)
	assert.Equal(3, result.Attempts)
	assert.Equal(int32(3), atomic.LoadInt32(&attempts))
	assert.Nil(uut.Stop(utCtxt))
}

func TestHTTPLiveStreamForwarderRetryExhaustionObservable(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()

	mockSQL := mocks.NewConnectionManager(t)
	mockDB := mocks.NewPersistenceManager(t)
	mockSQL.On("NewPersistanceManager").Return(mockDB).Maybe()
	mockDB.On("Close").Return().Maybe()
	mockSender := mocks.NewSegmentSender(t)

	retryCfg := fastRetryCfg()
	uut, err := forwarder.NewHTTPLiveStreamSegmentForwarder(
		utCtxt, mockSQL, mockSender, 2, retryCfg, nil,
	)
	assert.Nil(err)

	testSource := common.VideoSource{ID: uuid.NewString(), Streaming: 1}
	newSegment := newTestLiveSegment(testSource.ID)
	mockDB.On(
		"GetVideoSource",
		mock.AnythingOfType("*context.cancelCtx"),
		testSource.ID,
	).Return(testSource, nil)

	// First submission: every attempt fails
	mockSender.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Return(errors.New("persistent network failure")).Times(retryCfg.MaxAttempts)

	result, resErr := uut.ForwardSegmentAndWait(utCtxt, newSegment)
	assert.Nil(resErr)
	assert.Equal(forwarder.SegmentStatusFailed, result.Status)
	assert.Equal(retryCfg.MaxAttempts, result.Attempts)
	assert.NotNil(result.Err)

	// A later re-drive of the same failed segment must re-process it rather than reuse
	// the stale failure; this time it succeeds.
	mockSender.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Return(nil).Once()

	redriven, redriveErr := uut.ForwardSegmentAndWait(utCtxt, newSegment)
	assert.Nil(redriveErr)
	assert.Equal(forwarder.SegmentStatusProcessed, redriven.Status)
	assert.Equal(1, redriven.Attempts)

	assert.Nil(uut.Stop(utCtxt))
}

func TestHTTPLiveStreamForwarderConcurrentDuplicate(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()

	mockSQL := mocks.NewConnectionManager(t)
	mockDB := mocks.NewPersistenceManager(t)
	mockSQL.On("NewPersistanceManager").Return(mockDB).Maybe()
	mockDB.On("Close").Return().Maybe()
	mockSender := mocks.NewSegmentSender(t)

	uut, err := forwarder.NewHTTPLiveStreamSegmentForwarder(
		utCtxt, mockSQL, mockSender, 4, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	testSource := common.VideoSource{ID: uuid.NewString(), Streaming: 1}
	newSegment := newTestLiveSegment(testSource.ID)

	mockDB.On(
		"GetVideoSource",
		mock.AnythingOfType("*context.cancelCtx"),
		testSource.ID,
	).Return(testSource, nil)

	// Gate the single outbound call so all goroutines submit while it is in flight
	gate := make(chan struct{})
	var outboundCalls int32
	mockSender.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Run(func(args mock.Arguments) {
		atomic.AddInt32(&outboundCalls, 1)
		<-gate
	}).Return(nil).Once()

	const submissions = 8
	var wg sync.WaitGroup
	results := make([]forwarder.SegmentForwardResult, submissions)
	errs := make([]error, submissions)
	wg.Add(submissions)
	for i := 0; i < submissions; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = uut.ForwardSegmentAndWait(utCtxt, newSegment)
		}(i)
	}

	// Give every goroutine time to join the single in-flight run
	time.Sleep(50 * time.Millisecond)
	close(gate)

	doneCtxt, cancel := context.WithTimeout(utCtxt, 5*time.Second)
	defer cancel()
	completed := make(chan struct{})
	go func() {
		wg.Wait()
		close(completed)
	}()
	select {
	case <-doneCtxt.Done():
		t.Fatal("timed out waiting for concurrent submissions to complete")
	case <-completed:
	}

	assert.Equal(int32(1), atomic.LoadInt32(&outboundCalls))
	for i := 0; i < submissions; i++ {
		assert.Nil(errs[i])
		assert.Equal(forwarder.SegmentStatusProcessed, results[i].Status)
	}

	assert.Nil(uut.Stop(utCtxt))
}

func TestHTTPLiveStreamForwarderStopDrainAndCancel(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()

	mockSQL := mocks.NewConnectionManager(t)
	mockDB := mocks.NewPersistenceManager(t)
	mockSQL.On("NewPersistanceManager").Return(mockDB).Maybe()
	mockDB.On("Close").Return().Maybe()
	mockSender := mocks.NewSegmentSender(t)

	uut, err := forwarder.NewHTTPLiveStreamSegmentForwarder(
		utCtxt, mockSQL, mockSender, 2, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	testSource := common.VideoSource{ID: uuid.NewString(), Streaming: 1}
	newSegment := newTestLiveSegment(testSource.ID)

	mockDB.On(
		"GetVideoSource",
		mock.AnythingOfType("*context.cancelCtx"),
		testSource.ID,
	).Return(testSource, nil)

	// The in-flight segment completes while Stop is draining; it must finish with a
	// terminal result and Stop must return promptly instead of hanging.
	release := make(chan struct{})
	finished := make(chan struct{})
	mockSender.On(
		"ForwardSegment",
		mock.Anything,
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Run(func(args mock.Arguments) {
		<-release
		close(finished)
	}).Return(nil).Maybe()

	go func() {
		_, _ = uut.ForwardSegmentAndWait(utCtxt, newSegment)
	}()
	time.Sleep(50 * time.Millisecond)

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- uut.Stop(utCtxt)
	}()

	// Allow the in-flight request to finish during the drain window
	close(release)

	select {
	case stopErr := <-stopDone:
		assert.Nil(stopErr)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return while an in-flight segment completed")
	}
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight segment did not complete")
	}

	// Submissions after Stop must fail explicitly rather than block forever
	postErr := uut.ForwardSegment(utCtxt, newSegment, true)
	assert.NotNil(postErr)
}
