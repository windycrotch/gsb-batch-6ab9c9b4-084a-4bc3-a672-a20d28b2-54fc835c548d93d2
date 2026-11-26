package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v3"
)

// offer is kept for compatibility with code or tests that reference the
// historical package-level variable. It must not be used by OnConnect: every
// request decodes its own SessionDescription so concurrent offers cannot
// overwrite each other.
var offer webrtc.SessionDescription

// iceGatherWait bounds the time a non-trickle offer request waits for ICE
// candidates. Host candidates are gathered almost immediately; the bound only
// prevents an unreachable external STUN server from holding a request (and
// its half-open PeerConnection) indefinitely. Candidates gathered before the
// deadline are still embedded in the returned description.
const iceGatherWait = 5 * time.Second

// serverAPI builds PeerConnections with the default codecs/interceptors plus
// loopback ICE candidates. Loopback candidates are additive and make local
// RTP/HTTP verification robust where firewall policy filters LAN candidates;
// regular interface candidates are still gathered for real viewers.
var serverAPI = newServerAPI()

func newServerAPI() *webrtc.API {
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		log.Panicln(err)
	}

	interceptorRegistry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptorRegistry); err != nil {
		log.Panicln(err)
	}

	settingEngine := webrtc.SettingEngine{}
	settingEngine.SetIncludeLoopbackCandidate(true)

	return webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(interceptorRegistry),
		webrtc.WithSettingEngine(settingEngine),
	)
}

// viewerSession is one audience member's independent PeerConnection. Every
// offer owns its own instance so negotiation results can never cross between
// viewers. Teardown is idempotent and only touches this viewer's connection.
type viewerSession struct {
	id uint64
	pc *webrtc.PeerConnection

	closeOnce sync.Once
	// closed is closed as soon as teardown starts.
	closed chan struct{}
	// done is closed once PeerConnection.Close has returned.
	done chan struct{}
}

var (
	sessionsMu sync.Mutex
	sessions   = make(map[uint64]*viewerSession)
	sessionSeq uint64

	// trackMu guards lazy initialization of videoTrack/audioTrack.
	trackMu sync.Mutex
)

func registerSession(pc *webrtc.PeerConnection) *viewerSession {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()

	sessionSeq++
	session := &viewerSession{
		id:     sessionSeq,
		pc:     pc,
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	sessions[session.id] = session
	return session
}

func unregisterSession(id uint64) {
	sessionsMu.Lock()
	delete(sessions, id)
	sessionsMu.Unlock()
}

// activeViewerCount returns the number of viewer sessions that have been
// created and not yet torn down. Mainly useful for diagnostics and tests.
func activeViewerCount() int {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	return len(sessions)
}

// closeAllViewers tears down every active viewer connection. Used during
// graceful service shutdown. It never panics on an individual failure.
func closeAllViewers() {
	sessionsMu.Lock()
	all := make([]*viewerSession, 0, len(sessions))
	for _, session := range sessions {
		all = append(all, session)
	}
	sessionsMu.Unlock()

	for _, session := range all {
		session.shutdown()
	}
}

// shutdown closes exactly this viewer's PeerConnection and removes it from
// the registry. Safe to call multiple times and from PeerConnection
// callbacks.
func (s *viewerSession) shutdown() {
	s.closeOnce.Do(func() {
		close(s.closed)
		unregisterSession(s.id)

		if s.pc != nil {
			if err := s.pc.Close(); err != nil && !errors.Is(err, webrtc.ErrConnectionClosed) {
				log.Printf("Viewer %d: error while closing PeerConnection: %v", s.id, err)
			}
		}

		close(s.done)
		log.Printf("Viewer %d: session cleaned up", s.id)
	})
}

func initWebrtcTrack(mediaType string, codec string, trackName string) *webrtc.TrackLocalStaticRTP {
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: codec}, mediaType, trackName)

	if err != nil {
		log.Panicln(err)
	}

	return track
}

// ensureTracks lazily creates the configured local tracks. Production code
// (main) creates them up front; this also keeps OnConnect usable in tests or
// embeddings that only set the config.
func ensureTracks() {
	trackMu.Lock()
	defer trackMu.Unlock()

	if config.EnableVideo && videoTrack == nil {
		videoTrack = initWebrtcTrack("video", config.VideoCodec, config.VideoTrackName)
	}
	if config.EnableAudio && audioTrack == nil {
		audioTrack = initWebrtcTrack("audio", config.AudioCodec, config.AudioTrackName)
	}
}

func OnConnect(w http.ResponseWriter, r *http.Request) {
	// viewer is set once a PeerConnection exists; the panic safety net uses it
	// to release a half-built session for the offending request.
	var viewer *viewerSession
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("Recovered while handling offer: %v\n%s", rec, debug.Stack())
			if viewer != nil {
				viewer.shutdown()
			}
			// Any panic at this stage is treated as a bad/unnegotiable offer:
			// answer with a clear 4xx instead of dropping the connection. If a
			// response was already started this write is a no-op with a warning.
			http.Error(w, "Bad or unnegotiable session description", http.StatusBadRequest)
		}
	}()

	if config.AllowCrossOrigin {
		header := w.Header()
		header.Add("Access-Control-Allow-Origin", "*")
		header.Add("Access-Control-Allow-Methods", "DELETE, POST, GET, OPTIONS")
		header.Add("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
		header.Add("Access-Control-Allow-Private-Network", "Content-Type, Authorization, X-Requested-With")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	if r.Method != http.MethodPost {
		log.Println("Invalid HTTP method!")
		http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		return
	}

	// Decode into a request-local value: concurrent offers must never share
	// or overwrite one SessionDescription.
	var remoteOffer webrtc.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&remoteOffer); err != nil {
		log.Printf("Rejecting offer: malformed session description: %v", err)
		http.Error(w, "Bad session description", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(remoteOffer.SDP) == "" {
		log.Println("Rejecting offer: empty SDP")
		http.Error(w, "Bad session description: empty SDP", http.StatusBadRequest)
		return
	}

	ensureTracks()

	peerConnection, err := serverAPI.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: iceServersArray,
			},
		},
	})
	if err != nil {
		// A PeerConnection could not be created; nothing was registered yet.
		log.Printf("Unable to create PeerConnection: %v", err)
		http.Error(w, "Unable to create PeerConnection", http.StatusInternalServerError)
		return
	}

	// Register before any further work so every failure path and process
	// shutdown can reach the exact same idempotent cleanup.
	viewer = registerSession(peerConnection)

	cleanupOnStateChange(viewer, peerConnection)

	// dtlsTransport is shared by every sender of this PeerConnection; keep a
	// reference so a graceful viewer leave (DTLS close alert) reclaims the
	// session immediately instead of waiting for the ICE timeout.
	var dtlsTransport *webrtc.DTLSTransport

	if config.EnableVideo {
		videoSender, addErr := peerConnection.AddTrack(videoTrack)
		if addErr != nil {
			log.Printf("Viewer %d: unable to add video track: %v", viewer.id, addErr)
			viewer.shutdown()
			http.Error(w, "Unable to add video track", http.StatusInternalServerError)
			return
		}
		dtlsTransport = videoSender.Transport()
	}

	if config.EnableAudio {
		audioSender, addErr := peerConnection.AddTrack(audioTrack)
		if addErr != nil {
			log.Printf("Viewer %d: unable to add audio track: %v", viewer.id, addErr)
			viewer.shutdown()
			http.Error(w, "Unable to add audio track", http.StatusInternalServerError)
			return
		}
		if dtlsTransport == nil {
			dtlsTransport = audioSender.Transport()
		}
	}

	if dtlsTransport != nil {
		dtlsTransport.OnStateChange(func(state webrtc.DTLSTransportState) {
			if state == webrtc.DTLSTransportStateClosed || state == webrtc.DTLSTransportStateFailed {
				log.Printf("Viewer %d: DTLS transport state %s, cleaning up", viewer.id, state)
				// The callback runs while pion holds the DTLS transport lock,
				// and shutdown -> PeerConnection.Close tries to take that same
				// lock, so teardown must run in its own goroutine. shutdown is
				// idempotent and safe to race with other cleanup signals.
				go viewer.shutdown()
			}
		})
	}

	// An offer that cannot be negotiated is the client's fault (4xx). Close
	// the half-built PeerConnection before responding so no session or track
	// binding survives a rejected request.
	if err := peerConnection.SetRemoteDescription(remoteOffer); err != nil {
		log.Printf("Viewer %d: SetRemoteDescription failed: %v", viewer.id, err)
		viewer.shutdown()
		http.Error(w, "Invalid or unnegotiable session description", http.StatusBadRequest)
		return
	}

	answer, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		log.Printf("Viewer %d: CreateAnswer failed: %v", viewer.id, err)
		viewer.shutdown()
		http.Error(w, "Unable to create answer for the provided session description", http.StatusBadRequest)
		return
	}

	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)

	if err = peerConnection.SetLocalDescription(answer); err != nil {
		log.Printf("Viewer %d: SetLocalDescription failed: %v", viewer.id, err)
		viewer.shutdown()
		http.Error(w, "Unable to set local session description", http.StatusInternalServerError)
		return
	}

	// Wait for trickle candidates to gather so the answer is self-contained,
	// but do not wait forever: a gone client or a stalled STUN server must not
	// pin this goroutine/session. LocalDescription always embeds the
	// candidates gathered so far.
	timer := time.NewTimer(iceGatherWait)
	defer timer.Stop()

	select {
	case <-gatherComplete:
	case <-timer.C:
		log.Printf("Viewer %d: ICE gathering not complete after %s, returning candidates gathered so far", viewer.id, iceGatherWait)
	case <-r.Context().Done():
		log.Printf("Viewer %d: HTTP request abandoned before answer was sent: %v", viewer.id, r.Context().Err())
		viewer.shutdown()
		return
	case <-viewer.closed:
		// Teardown triggered externally while gathering.
		return
	}

	localDescription := peerConnection.LocalDescription()
	if localDescription == nil {
		log.Printf("Viewer %d: local description is unexpectedly nil", viewer.id)
		viewer.shutdown()
		http.Error(w, "Unable to build answer", http.StatusInternalServerError)
		return
	}

	response, err := json.Marshal(localDescription)
	if err != nil {
		log.Printf("Viewer %d: unable to marshal answer: %v", viewer.id, err)
		viewer.shutdown()
		http.Error(w, "Unable to marshal answer", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(response); err != nil {
		// The client likely went away. Tear the session down rather than
		// leaving a PeerConnection that will never be watched.
		log.Printf("Viewer %d: unable to write answer: %v", viewer.id, err)
		viewer.shutdown()
		return
	}
}

// cleanupOnStateChange closes the viewer as soon as its connection reaches a
// terminal/dead state. Only this viewer's own PeerConnection is closed; the
// shared local tracks stay alive for every other viewer, and pion removes just
// this sender's binding from the shared track.
func cleanupOnStateChange(viewer *viewerSession, peerConnection *webrtc.PeerConnection) {
	peerConnection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("Viewer %d: connection state changed: %s", viewer.id, state)
		switch state {
		case webrtc.PeerConnectionStateDisconnected,
			webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateClosed:
			log.Printf("Viewer %d: closing connection after state %s", viewer.id, state)
			viewer.shutdown()
		}
	})
}
