package control

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Glimesh/waveguide/pkg/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeService records every external lifecycle call so tests can assert that
// stop requests are sent exactly once per stream.
type fakeService struct {
	mu sync.Mutex

	hmacKey []byte

	startCalls []types.ChannelID
	endCalls   []types.StreamID

	startFail bool
}

func (f *fakeService) SetLogger(_ logrus.FieldLogger) {}

func (f *fakeService) Name() string { return "fake-service" }

func (f *fakeService) Connect() error { return nil }

func (f *fakeService) GetHmacKey(_ types.ChannelID) ([]byte, error) {
	return f.hmacKey, nil
}

func (f *fakeService) StartStream(channelID types.ChannelID) (types.StreamID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.startFail {
		return 0, fmt.Errorf("forced start failure")
	}
	f.startCalls = append(f.startCalls, channelID)
	// Mirror the dummy service behaviour: stream ID derives from the channel.
	return types.StreamID(channelID + 1), nil
}

func (f *fakeService) EndStream(streamID types.StreamID) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.endCalls = append(f.endCalls, streamID)
	return nil
}

func (f *fakeService) UpdateStreamMetadata(_ types.StreamID, _ types.StreamMetadata) error {
	return nil
}

func (f *fakeService) SendJpegPreviewImage(_ types.StreamID, _ []byte) error {
	return nil
}

func (f *fakeService) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.startCalls)
}

func (f *fakeService) endCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.endCalls)
}

// fakeOrchestrator mirrors fakeService for orchestrator stop notifications.
type fakeOrchestrator struct {
	mu sync.Mutex

	startCalls []types.StreamID
	stopCalls  []types.StreamID
}

func (f *fakeOrchestrator) SetLogger(_ logrus.FieldLogger) {}

func (f *fakeOrchestrator) Name() string { return "fake-orchestrator" }

func (f *fakeOrchestrator) Connect() error { return nil }

func (f *fakeOrchestrator) Close() error { return nil }

func (f *fakeOrchestrator) StartStream(_ types.ChannelID, streamID types.StreamID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startCalls = append(f.startCalls, streamID)
	return nil
}

func (f *fakeOrchestrator) StopStream(_ types.ChannelID, streamID types.StreamID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopCalls = append(f.stopCalls, streamID)
	return nil
}

func (f *fakeOrchestrator) Heartbeat(_ types.ChannelID) error { return nil }

func (f *fakeOrchestrator) stopCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.stopCalls)
}

// newTestControl builds a Control wired to recording fakes and an in-process
// HTTP server. A stub WHEP endpoint is registered because Stream.Ingest dials
// the control's WHEP URL; the stub completes a minimal (media-less) handshake so
// the ingest goroutine stays alive until the stream is stopped.
func newTestControl(t *testing.T) (*Control, *fakeService, *fakeOrchestrator, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	svc := &fakeService{hmacKey: []byte("test-key")}
	or := &fakeOrchestrator{}

	ctrl := NewWithDependencies(ctx, svc, or, "testhost", logger)

	mux := ctrl.HTTPHandler().(*http.ServeMux)
	registerStubWHEP(mux)

	server := &http.Server{Handler: ctrl.HTTPHandler()}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()

	ctrl.HTTPServerType = "http"
	ctrl.HTTPAddress = listener.Addr().String()

	t.Cleanup(func() {
		_ = server.Close()
		cancel()
	})

	return ctrl, svc, or, cancel
}

// stubWhepOffer is a minimal but spec-shaped SDP offer Stream.Ingest can parse
// and answer. The media line is inactive so no ICE/DTLS is attempted.
const stubWhepOffer = "v=0\r\n" +
	"o=- 0 0 IN IP4 127.0.0.1\r\n" +
	"s=-\r\n" +
	"t=0 0\r\n" +
	"m=video 9 UDP/TLS/RTP/SAVPF 96\r\n" +
	"c=IN IP4 0.0.0.0\r\n" +
	"a=mid:0\r\n" +
	"a=inactive\r\n" +
	"a=ice-ufrag:stub\r\n" +
	"a=ice-pwd:stubpasswordstubpassword\r\n" +
	"a=fingerprint:sha-256 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00\r\n" +
	"a=setup:actpass\r\n" +
	"a=rtpmap:96 VP8/90000\r\n"

// registerStubWHEP answers the two HTTP calls Stream.Ingest makes: an offer
// fetch on the endpoint and an answer POST on the resource URL.
func registerStubWHEP(mux *http.ServeMux) {
	mux.HandleFunc("/whep/endpoint/", func(w http.ResponseWriter, r *http.Request) {
		// Ingest POSTs the answer back to an absolute Location URL.
		w.Header().Set("Location", "http://"+r.Host+"/whep/resource/stub")
		w.Header().Set("Content-Type", "application/sdp")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(stubWhepOffer))
	})
	mux.HandleFunc("/whep/resource/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
}

func TestStartStopStreamLifecycle(t *testing.T) {
	ctrl, svc, or, _ := newTestControl(t)
	channelID := types.ChannelID(100)

	stream, err := ctrl.StartStream(channelID)
	require.NoError(t, err)
	require.NotNil(t, stream)
	assert.True(t, ctrl.HasStream(channelID))
	assert.Equal(t, 1, svc.startCount())
	assert.Equal(t, 1, len(or.startCalls))

	require.NoError(t, ctrl.StopStream(channelID))

	// After the last session ends the channel must be back to not-live.
	assert.False(t, ctrl.HasStream(channelID))
	assert.True(t, stream.Stopped())
	assert.Equal(t, 1, svc.endCount(), "external stop request sent once")
	assert.Equal(t, 1, or.stopCount(), "orchestrator stop request sent once")

	// Stopping again is a no-op: no duplicate external requests.
	require.NoError(t, ctrl.StopStream(channelID))
	require.NoError(t, ctrl.StopStream(channelID))
	assert.Equal(t, 1, svc.endCount())
	assert.Equal(t, 1, or.stopCount())

	// The same channel can immediately publish again.
	stream2, err := ctrl.StartStream(channelID)
	require.NoError(t, err)
	assert.True(t, ctrl.HasStream(channelID))
	assert.False(t, stream2.Stopped())

	require.NoError(t, ctrl.StopStream(channelID))
	assert.False(t, ctrl.HasStream(channelID))
	assert.Equal(t, 2, svc.endCount(), "one stop per distinct stream")
}

func TestConcurrentStartOnlyOneWins(t *testing.T) {
	ctrl, svc, or, _ := newTestControl(t)
	channelID := types.ChannelID(200)

	const attempts = 16
	var wg sync.WaitGroup
	errs := make(chan error, attempts)
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			_, err := ctrl.StartStream(channelID)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	successes, failures := 0, 0
	for err := range errs {
		if err == nil {
			successes++
		} else {
			failures++
		}
	}
	assert.Equal(t, 1, successes, "exactly one stream registered")
	assert.Equal(t, attempts-1, failures, "all overlapping requests get an explicit failure")
	assert.True(t, ctrl.HasStream(channelID))

	require.NoError(t, ctrl.StopStream(channelID))
	assert.False(t, ctrl.HasStream(channelID))
	// Only the single winning stream ever notified the external services.
	assert.Equal(t, 1, svc.startCount())
	assert.Equal(t, 1, svc.endCount())
	assert.Equal(t, 1, or.stopCount())
}

func TestConcurrentStopSendsExternalRequestOnce(t *testing.T) {
	ctrl, svc, or, _ := newTestControl(t)
	channelID := types.ChannelID(300)

	_, err := ctrl.StartStream(channelID)
	require.NoError(t, err)

	const stoppers = 32
	var wg sync.WaitGroup
	wg.Add(stoppers)
	for i := 0; i < stoppers; i++ {
		go func() {
			defer wg.Done()
			_ = ctrl.StopStream(channelID)
		}()
	}
	wg.Wait()

	assert.False(t, ctrl.HasStream(channelID))
	assert.Equal(t, 1, svc.endCount(), "service stop exactly once under concurrent cleanup")
	assert.Equal(t, 1, or.stopCount(), "orchestrator stop exactly once under concurrent cleanup")
}

func TestStartFailureLeavesNoState(t *testing.T) {
	ctrl, svc, or, _ := newTestControl(t)
	channelID := types.ChannelID(400)
	svc.startFail = true

	_, err := ctrl.StartStream(channelID)
	require.Error(t, err)
	assert.False(t, ctrl.HasStream(channelID), "failed start must not leave a registered stream")

	// After the failure the channel can still be used.
	svc.startFail = false
	_, err = ctrl.StartStream(channelID)
	require.NoError(t, err)
	assert.True(t, ctrl.HasStream(channelID))
	require.NoError(t, ctrl.StopStream(channelID))
	assert.False(t, ctrl.HasStream(channelID))
	assert.Equal(t, 1, or.stopCount())
}

func TestShutdownStopsAllStreams(t *testing.T) {
	ctrl, svc, or, _ := newTestControl(t)

	for _, id := range []types.ChannelID{1, 2, 3} {
		_, err := ctrl.StartStream(id)
		require.NoError(t, err)
	}

	ctrl.Shutdown()

	assert.False(t, ctrl.HasStream(1))
	assert.False(t, ctrl.HasStream(2))
	assert.False(t, ctrl.HasStream(3))
	assert.Equal(t, 3, svc.endCount())
	assert.Equal(t, 3, or.stopCount())

	// Shutdown must be safe to repeat.
	ctrl.Shutdown()
	assert.Equal(t, 3, svc.endCount())
}

func TestStreamStopIsIdempotent(t *testing.T) {
	ctrl, _, _, _ := newTestControl(t)
	stream, err := ctrl.StartStream(types.ChannelID(500))
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); stream.Stop() }()
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stream.Stop calls deadlocked")
	}
	assert.True(t, stream.Stopped())
	require.NoError(t, ctrl.StopStream(types.ChannelID(500)))
}
