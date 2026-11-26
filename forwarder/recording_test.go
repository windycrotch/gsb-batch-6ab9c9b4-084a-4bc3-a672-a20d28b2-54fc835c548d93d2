package forwarder_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alwitt/livemix/common"
	"github.com/alwitt/livemix/common/ipc"
	"github.com/alwitt/livemix/forwarder"
	"github.com/alwitt/livemix/hls"
	"github.com/alwitt/livemix/mocks"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func newTestRecordingSegment(sourceID string) common.VideoSegmentWithData {
	return common.VideoSegmentWithData{
		VideoSegment: common.VideoSegment{
			ID:       uuid.NewString(),
			SourceID: sourceID,
			Segment:  hls.Segment{Name: fmt.Sprintf("%s.ts", uuid.NewString())},
		},
		Content: []byte(uuid.NewString()),
	}
}

func recordingForwarderDeps(t *testing.T) (
	*mocks.ConnectionManager,
	*mocks.PersistenceManager,
	*mocks.SegmentSender,
	*mocks.Broadcaster,
	common.RecordingStorageConfig,
) {
	mockSQL := mocks.NewConnectionManager(t)
	mockDB := mocks.NewPersistenceManager(t)
	// Optional: tests that never process a segment make no DB calls
	mockSQL.On("NewPersistanceManager").Return(mockDB).Maybe()
	mockDB.On("Close").Return().Maybe()
	mockS3 := mocks.NewSegmentSender(t)
	mockBroadcaster := mocks.NewBroadcaster(t)
	storageCfg := common.RecordingStorageConfig{
		StorageBucket: uuid.NewString(), StorageObjectPrefix: uuid.NewString(),
	}
	return mockSQL, mockDB, mockS3, mockBroadcaster, storageCfg
}

// expectActiveRecordings mocks the per-recording session state lookup
func expectActiveRecordings(
	mockDB *mocks.PersistenceManager, recordings []string, active bool,
) {
	for _, id := range recordings {
		session := common.Recording{ID: id, Active: -1}
		if active {
			session.Active = 1
		}
		mockDB.On(
			"GetRecordingSession",
			mock.AnythingOfType("*context.cancelCtx"),
			id,
		).Return(session, nil).Maybe()
	}
}

func TestS3RecordingSegmentForwarder(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()
	mockSQL, mockDB, mockS3, mockBroadcaster, storageCfg := recordingForwarderDeps(t)

	uut, err := forwarder.NewS3RecordingSegmentForwarder(
		utCtxt, storageCfg, mockSQL, mockS3, mockBroadcaster, 2, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	// ------------------------------------------------------------------------------------
	// Case 0: active recordings, segments are uploaded then broadcast once each

	{
		testSourceID := uuid.NewString()
		testRecordings := []string{uuid.NewString(), uuid.NewString()}
		expectActiveRecordings(mockDB, testRecordings, true)

		testSegments := []common.VideoSegmentWithData{}
		testSegmentByID := map[string]common.VideoSegmentWithData{}
		for itr := 0; itr < 2; itr++ {
			segment := newTestRecordingSegment(testSourceID)
			testSegments = append(testSegments, segment)
			testSegmentByID[segment.ID] = segment
		}
		broadcasts := make(chan ipc.RecordingSegmentReport, 2)

		mockS3.On(
			"ForwardSegment",
			mock.AnythingOfType("*context.cancelCtx"),
			mock.AnythingOfType("common.VideoSegmentWithData"),
		).Run(func(args mock.Arguments) {
			segment, ok := args.Get(1).(common.VideoSegmentWithData)
			assert.True(ok)
			assert.Contains(testSegmentByID, segment.ID)
		}).Return(nil).Times(len(testSegments))
		mockBroadcaster.On(
			"Broadcast",
			mock.AnythingOfType("*context.cancelCtx"),
			mock.Anything,
		).Run(func(args mock.Arguments) {
			msg, ok := args.Get(1).(*ipc.RecordingSegmentReport)
			assert.True(ok)
			broadcasts <- *msg
		}).Return(nil).Times(len(testSegments))

		assert.Nil(uut.ForwardSegment(utCtxt, testRecordings, testSegments))

		lclCtxt, lclCtxtCancel := context.WithTimeout(utCtxt, 3*time.Second)
		defer lclCtxtCancel()
		receivedBroadcast := map[string]bool{}
		for itr := 0; itr < 2; itr++ {
			select {
			case <-lclCtxt.Done():
				t.Fatal("timed out waiting for broadcast")
			case msg := <-broadcasts:
				assert.EqualValues(testRecordings, msg.RecordingIDs)
				assert.Len(msg.Segments, 1)
				receivedBroadcast[msg.Segments[0].ID] = true
			}
		}
		assert.Len(receivedBroadcast, len(testSegments))
		assert.Empty(uut.FailedSegments())
	}

	// ------------------------------------------------------------------------------------
	// Case 1: pre-uploaded segment skips upload but is still broadcast once

	{
		testSourceID := uuid.NewString()
		testRecordings := []string{uuid.NewString()}
		expectActiveRecordings(mockDB, testRecordings, true)

		uploaded := 1
		segment := newTestRecordingSegment(testSourceID)
		segment.Uploaded = &uploaded
		broadcasts := make(chan ipc.RecordingSegmentReport, 1)

		mockBroadcaster.On(
			"Broadcast",
			mock.AnythingOfType("*context.cancelCtx"),
			mock.Anything,
		).Run(func(args mock.Arguments) {
			msg, ok := args.Get(1).(*ipc.RecordingSegmentReport)
			assert.True(ok)
			broadcasts <- *msg
		}).Return(nil).Once()

		assert.Nil(uut.ForwardSegment(utCtxt, testRecordings, []common.VideoSegmentWithData{segment}))
		select {
		case <-broadcasts:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for broadcast")
		}
	}

	assert.Nil(uut.Stop(utCtxt))
}

func TestS3RecordingSegmentForwarderSkippedWhenRecordingInactive(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()
	mockSQL, mockDB, mockS3, mockBroadcaster, storageCfg := recordingForwarderDeps(t)

	uut, err := forwarder.NewS3RecordingSegmentForwarder(
		utCtxt, storageCfg, mockSQL, mockS3, mockBroadcaster, 2, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	testSourceID := uuid.NewString()
	stoppedRecording := uuid.NewString()
	// Recording session already ended
	expectActiveRecordings(mockDB, []string{stoppedRecording}, false)

	segment := newTestRecordingSegment(testSourceID)
	assert.Nil(uut.ForwardSegment(
		utCtxt, []string{stoppedRecording}, []common.VideoSegmentWithData{segment},
	))

	// Give the worker time to process; no S3 / broadcast mock expectations exist, so any
	// such call fails the test during cleanup.
	time.Sleep(100 * time.Millisecond)
	assert.Empty(uut.FailedSegments())
	assert.Nil(uut.Stop(utCtxt))
}

func TestS3RecordingSegmentForwarderUploadRetryThenSuccess(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()
	mockSQL, mockDB, mockS3, mockBroadcaster, storageCfg := recordingForwarderDeps(t)

	retryCfg := fastRetryCfg()
	uut, err := forwarder.NewS3RecordingSegmentForwarder(
		utCtxt, storageCfg, mockSQL, mockS3, mockBroadcaster, 2, retryCfg, nil,
	)
	assert.Nil(err)

	testSourceID := uuid.NewString()
	testRecordings := []string{uuid.NewString()}
	expectActiveRecordings(mockDB, testRecordings, true)
	segment := newTestRecordingSegment(testSourceID)

	var uploadAttempts int32
	mockS3.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Run(func(args mock.Arguments) {
		atomic.AddInt32(&uploadAttempts, 1)
	}).Return(func(context.Context, common.VideoSegmentWithData) error {
		if atomic.LoadInt32(&uploadAttempts) < 3 {
			return errors.New("S3 put failed")
		}
		return nil
	})

	broadcasts := make(chan ipc.RecordingSegmentReport, 1)
	mockBroadcaster.On(
		"Broadcast",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.Anything,
	).Run(func(args mock.Arguments) {
		broadcasts <- *args.Get(1).(*ipc.RecordingSegmentReport)
	}).Return(nil).Once()

	assert.Nil(uut.ForwardSegment(
		utCtxt, testRecordings, []common.VideoSegmentWithData{segment},
	))
	select {
	case <-broadcasts:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for broadcast after retry")
	}
	assert.Equal(int32(3), atomic.LoadInt32(&uploadAttempts))
	assert.Empty(uut.FailedSegments())
	assert.Nil(uut.Stop(utCtxt))
}

func TestS3RecordingSegmentForwarderBroadcastRetryNoReupload(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()
	mockSQL, mockDB, mockS3, mockBroadcaster, storageCfg := recordingForwarderDeps(t)

	uut, err := forwarder.NewS3RecordingSegmentForwarder(
		utCtxt, storageCfg, mockSQL, mockS3, mockBroadcaster, 2, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	testSourceID := uuid.NewString()
	testRecordings := []string{uuid.NewString()}
	expectActiveRecordings(mockDB, testRecordings, true)
	segment := newTestRecordingSegment(testSourceID)

	// Upload happens exactly once
	mockS3.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Return(nil).Once()

	var broadcastAttempts int32
	broadcasts := make(chan ipc.RecordingSegmentReport, 1)
	mockBroadcaster.On(
		"Broadcast",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.Anything,
	).Run(func(args mock.Arguments) {
		attempt := atomic.AddInt32(&broadcastAttempts, 1)
		if attempt == 3 {
			broadcasts <- *args.Get(1).(*ipc.RecordingSegmentReport)
		}
	}).Return(func(context.Context, interface{}) error {
		if atomic.LoadInt32(&broadcastAttempts) < 3 {
			return errors.New("pubsub publish failed")
		}
		return nil
	})

	assert.Nil(uut.ForwardSegment(
		utCtxt, testRecordings, []common.VideoSegmentWithData{segment},
	))
	select {
	case <-broadcasts:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for broadcast")
	}
	assert.Equal(int32(3), atomic.LoadInt32(&broadcastAttempts))
	assert.Empty(uut.FailedSegments())
	assert.Nil(uut.Stop(utCtxt))
}

func TestS3RecordingSegmentForwarderRetryExhaustionObservable(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()
	mockSQL, mockDB, mockS3, mockBroadcaster, storageCfg := recordingForwarderDeps(t)

	retryCfg := fastRetryCfg()
	uut, err := forwarder.NewS3RecordingSegmentForwarder(
		utCtxt, storageCfg, mockSQL, mockS3, mockBroadcaster, 2, retryCfg, nil,
	)
	assert.Nil(err)

	testSourceID := uuid.NewString()
	testRecordings := []string{uuid.NewString()}
	expectActiveRecordings(mockDB, testRecordings, true)
	segment := newTestRecordingSegment(testSourceID)

	mockS3.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Return(errors.New("S3 unavailable")).Times(retryCfg.MaxAttempts)
	// No broadcast expectation: a failed upload must never be announced as stored

	assert.Nil(uut.ForwardSegment(
		utCtxt, testRecordings, []common.VideoSegmentWithData{segment},
	))

	// Poll for the observable failure record instead of sleeping a fixed duration
	deadline := time.Now().Add(5 * time.Second)
	var failures []forwarder.FailedSegmentRecord
	for time.Now().Before(deadline) {
		failures = uut.FailedSegments()
		if len(failures) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Len(failures, 1)
	if len(failures) == 1 {
		assert.Equal(segment.ID, failures[0].SegmentID)
		assert.Equal(retryCfg.MaxAttempts, failures[0].Attempts)
		assert.NotNil(failures[0].Err)
	}

	// Re-drive the failed segment: it must be re-processed, succeed on one attempt, and
	// broadcast exactly once; the earlier failure record must clear.
	mockS3.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Return(nil).Once()
	broadcasts := make(chan ipc.RecordingSegmentReport, 1)
	mockBroadcaster.On(
		"Broadcast",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.Anything,
	).Run(func(args mock.Arguments) {
		broadcasts <- *args.Get(1).(*ipc.RecordingSegmentReport)
	}).Return(nil).Once()

	assert.Nil(uut.ForwardSegment(
		utCtxt, testRecordings, []common.VideoSegmentWithData{segment},
	))
	select {
	case <-broadcasts:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for broadcast after re-drive")
	}
	assert.Empty(uut.FailedSegments())

	assert.Nil(uut.Stop(utCtxt))
}

func TestS3RecordingSegmentForwarderConcurrentDuplicate(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()
	mockSQL, mockDB, mockS3, mockBroadcaster, storageCfg := recordingForwarderDeps(t)

	uut, err := forwarder.NewS3RecordingSegmentForwarder(
		utCtxt, storageCfg, mockSQL, mockS3, mockBroadcaster, 4, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	testSourceID := uuid.NewString()
	testRecordings := []string{uuid.NewString()}
	expectActiveRecordings(mockDB, testRecordings, true)
	segment := newTestRecordingSegment(testSourceID)

	gate := make(chan struct{})
	var uploadCalls int32
	var broadcastCalls int32
	mockS3.On(
		"ForwardSegment",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Run(func(args mock.Arguments) {
		atomic.AddInt32(&uploadCalls, 1)
		<-gate
	}).Return(nil).Once()
	mockBroadcaster.On(
		"Broadcast",
		mock.AnythingOfType("*context.cancelCtx"),
		mock.Anything,
	).Run(func(args mock.Arguments) {
		atomic.AddInt32(&broadcastCalls, 1)
	}).Return(nil).Once()

	const submissions = 8
	var wg sync.WaitGroup
	wg.Add(submissions)
	for i := 0; i < submissions; i++ {
		go func() {
			defer wg.Done()
			_ = uut.ForwardSegment(
				utCtxt, testRecordings, []common.VideoSegmentWithData{segment},
			)
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(gate)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent submissions")
	}

	// Allow the single owner run to finish upload + broadcast
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&broadcastCalls) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(int32(1), atomic.LoadInt32(&uploadCalls))
	assert.Equal(int32(1), atomic.LoadInt32(&broadcastCalls))

	assert.Nil(uut.Stop(utCtxt))
}

func TestS3RecordingSegmentForwarderStopPreventsLateBroadcast(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()
	mockSQL, _, mockS3, mockBroadcaster, storageCfg := recordingForwarderDeps(t)

	uut, err := forwarder.NewS3RecordingSegmentForwarder(
		utCtxt, storageCfg, mockSQL, mockS3, mockBroadcaster, 2, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	// Stopping with no in-flight work must return promptly
	stopDone := make(chan error, 1)
	go func() {
		stopDone <- uut.Stop(utCtxt)
	}()
	select {
	case stopErr := <-stopDone:
		assert.Nil(stopErr)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung with no in-flight work")
	}

	// After Stop, submissions are rejected and no upload / broadcast can occur
	segment := newTestRecordingSegment(uuid.NewString())
	subErr := uut.ForwardSegment(
		utCtxt, []string{uuid.NewString()}, []common.VideoSegmentWithData{segment},
	)
	assert.NotNil(subErr)
}

func TestS3RecordingSegmentForwarderStopDuringInflight(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.ErrorLevel)

	utCtxt := context.Background()
	mockSQL, mockDB, mockS3, mockBroadcaster, storageCfg := recordingForwarderDeps(t)

	uut, err := forwarder.NewS3RecordingSegmentForwarder(
		utCtxt, storageCfg, mockSQL, mockS3, mockBroadcaster, 2, fastRetryCfg(), nil,
	)
	assert.Nil(err)

	testSourceID := uuid.NewString()
	testRecordings := []string{uuid.NewString()}
	expectActiveRecordings(mockDB, testRecordings, true)
	segment := newTestRecordingSegment(testSourceID)

	release := make(chan struct{})
	uploadEntered := make(chan struct{})
	var once sync.Once
	mockS3.On(
		"ForwardSegment",
		mock.Anything,
		mock.AnythingOfType("common.VideoSegmentWithData"),
	).Run(func(args mock.Arguments) {
		once.Do(func() { close(uploadEntered) })
		<-release
	}).Return(nil).Maybe()
	// Broadcast is optional: if shutdown wins the race, it must never be called; if the
	// upload completes during drain, exactly one broadcast is allowed.
	var broadcastCalls int32
	mockBroadcaster.On(
		"Broadcast",
		mock.Anything,
		mock.Anything,
	).Run(func(args mock.Arguments) {
		atomic.AddInt32(&broadcastCalls, 1)
	}).Return(nil).Maybe()

	assert.Nil(uut.ForwardSegment(
		utCtxt, testRecordings, []common.VideoSegmentWithData{segment},
	))
	select {
	case <-uploadEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("upload never started")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- uut.Stop(utCtxt)
	}()

	// Finish the upload while Stop is draining; the segment either completes with one
	// broadcast or is cancelled, but Stop must return and no more than one broadcast.
	close(release)
	select {
	case stopErr := <-stopDone:
		assert.Nil(stopErr)
	case <-time.After(15 * time.Second):
		t.Fatal("Stop did not return for in-flight segment")
	}

	time.Sleep(100 * time.Millisecond)
	assert.LessOrEqual(int(atomic.LoadInt32(&broadcastCalls)), 1)
}
