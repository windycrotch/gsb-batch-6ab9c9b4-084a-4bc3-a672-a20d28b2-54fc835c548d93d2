package whip

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Glimesh/waveguide/pkg/control"
	"github.com/Glimesh/waveguide/pkg/types"
	"github.com/pion/webrtc/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// recordingService is a service.Service fake counting external lifecycle calls.
type recordingService struct {
	mu        sync.Mutex
	startHits int
	endHits   int
}

func (r *recordingService) SetLogger(_ logrus.FieldLogger) {}
func (r *recordingService) Name() string                   { return "recording-service" }
func (r *recordingService) Connect() error                 { return nil }

func (r *recordingService) GetHmacKey(channelID types.ChannelID) ([]byte, error) {
	h := sha256.New()
	h.Write([]byte(fmt.Sprint(channelID)))
	return []byte(fmt.Sprintf("%x", h.Sum(nil))), nil
}

func (r *recordingService) StartStream(channelID types.ChannelID) (types.StreamID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startHits++
	return types.StreamID(channelID + 1), nil
}

func (r *recordingService) EndStream(_ types.StreamID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.endHits++
	return nil
}

func (r *recordingService) UpdateStreamMetadata(_ types.StreamID, _ types.StreamMetadata) error {
	return nil
}

func (r *recordingService) SendJpegPreviewImage(_ types.StreamID, _ []byte) error {
	return nil
}

func (r *recordingService) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.startHits
}

func (r *recordingService) ends() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.endHits
}

// recordingOrchestrator counts orchestrator stop notifications.
type recordingOrchestrator struct {
	mu       sync.Mutex
	stopHits int
}

func (r *recordingOrchestrator) SetLogger(_ logrus.FieldLogger) {}
func (r *recordingOrchestrator) Name() string                   { return "recording-orchestrator" }
func (r *recordingOrchestrator) Connect() error                 { return nil }
func (r *recordingOrchestrator) Close() error                   { return nil }
func (r *recordingOrchestrator) StartStream(_ types.ChannelID, _ types.StreamID) error {
	return nil
}

func (r *recordingOrchestrator) StopStream(_ types.ChannelID, _ types.StreamID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopHits++
	return nil
}

func (r *recordingOrchestrator) Heartbeat(_ types.ChannelID) error { return nil }

func (r *recordingOrchestrator) stops() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopHits
}

type whipHarness struct {
	server *httptest.Server
	ctrl   *control.Control
	source *Source
	svc    *recordingService
	or     *recordingOrchestrator
	cancel context.CancelFunc
}

// newWhipHarness wires a real WHIP Source to a Control with recording fakes and
// a stub WHEP endpoint (Stream.Ingest dials WHEP) behind an in-process server.
func newWhipHarness(t *testing.T) *whipHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	svc := &recordingService{}
	or := &recordingOrchestrator{}
	ctrl := control.NewWithDependencies(ctx, svc, or, "testhost", logger)
	if os.Getenv("WHIP_TEST_LOG") != "" {
		logger.SetLevel(logrus.DebugLevel)
	}

	mux := ctrl.HTTPHandler().(*http.ServeMux)
	registerStubWHEP(mux)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(ctrl.HTTPHandler())
	server.Listener = listener
	server.Start()

	ctrl.HTTPServerType = "http"
	ctrl.HTTPAddress = server.Listener.Addr().String()

	source := New("", "", "")
	source.SetControl(ctrl)
	source.SetLogger(logger.WithField("input", "whip"))
	source.Listen(ctx)

	t.Cleanup(func() {
		server.Close()
		cancel()
	})

	return &whipHarness{
		server: server,
		ctrl:   ctrl,
		source: source,
		svc:    svc,
		or:     or,
		cancel: cancel,
	}
}

// stubWhepOffer is a minimal but spec-shaped SDP offer Stream.Ingest can parse
// and answer. The media line is inactive so no real ICE/DTLS is attempted,
// which keeps the ingest goroutine alive until the stream is stopped.
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

// registerStubWHEP answers Stream.Ingest's WHEP handshake so the ingest
// goroutine stays alive until the stream is stopped.
func registerStubWHEP(mux *http.ServeMux) {
	mux.HandleFunc("/whep/endpoint/", func(w http.ResponseWriter, r *http.Request) {
		// Ingest POSTs the answer back to this absolute Location URL. Requests
		// are plain HTTP in tests, so derive the host from the inbound request.
		w.Header().Set("Location", "http://"+r.Host+"/whep/resource/stub")
		w.Header().Set("Content-Type", "application/sdp")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(stubWhepOffer))
	})
	mux.HandleFunc("/whep/resource/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
}

func (h *whipHarness) endpoint(channelID int) string {
	return h.server.URL + "/whip/endpoint/" + fmt.Sprint(channelID)
}

// streamKeyFor mirrors the dummy service key derivation used by the fake.
func streamKeyFor(channelID int) string {
	sum := sha256.Sum256([]byte(fmt.Sprint(channelID)))
	return fmt.Sprintf("%x", sum[:])
}

// validOfferSDP builds a real WebRTC offer the WHIP endpoint can negotiate.
func validOfferSDP(t *testing.T) string {
	t.Helper()

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer func() { _ = pc.Close() }()

	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
		_, err = pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionSendrecv,
		})
		require.NoError(t, err)
	}

	offer, err := pc.CreateOffer(nil)
	require.NoError(t, err)
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	require.NoError(t, pc.SetLocalDescription(offer))
	<-gatherComplete

	return pc.LocalDescription().SDP
}

func (h *whipHarness) publish(t *testing.T, channelID int) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.endpoint(channelID), strings.NewReader(validOfferSDP(t)))
	require.NoError(t, err)
	req.Header.Set("Authorization", streamKeyFor(channelID))
	req.Header.Set("Content-Type", "application/sdp")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func (h *whipHarness) delete(channelID int, auth string) *http.Response {
	url := h.endpoint(channelID)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		panic(fmt.Sprintf("build delete request: %v", err))
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(fmt.Sprintf("send delete request: %v", err))
	}
	return resp
}

// waitFor asserts on a condition with a deadline so asynchronous teardown
// (connection callbacks, timeout timers) can be tested deterministically.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !condition() {
		t.Fatalf("timed out waiting for condition: %s", message)
	}
}

// waitForEnds blocks until the fake service has recorded exactly n external
// stop requests. The control registry is removed before EndStream runs, so
// asserting on stream absence alone can race the in-flight external call.
func (h *whipHarness) waitForEnds(t *testing.T, n int) {
	t.Helper()
	waitFor(t, 2*time.Second, func() bool { return h.svc.ends() == n },
		fmt.Sprintf("service EndStream called %d time(s), got %d", n, h.svc.ends()))
}

// waitForOrStops blocks until the fake orchestrator recorded n stop requests.
func (h *whipHarness) waitForOrStops(t *testing.T, n int) {
	t.Helper()
	waitFor(t, 2*time.Second, func() bool { return h.or.stops() == n },
		fmt.Sprintf("orchestrator StopStream called %d time(s), got %d", n, h.or.stops()))
}
