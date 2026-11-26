package whip

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Glimesh/waveguide/pkg/control"
	"github.com/Glimesh/waveguide/pkg/types"

	"github.com/pion/webrtc/v3"
	"github.com/sirupsen/logrus"
)

// PC_TIMEOUT is how long a negotiated peer connection has to reach the
// connected state before it is considered stale and torn down.
const PC_TIMEOUT = time.Minute * 5

// session represents a single WHIP publishing attempt. Every lifecycle path
// (client DELETE, ICE/connection failure, connect timeout) is keyed on the
// concrete session, so teardown of an old session can never touch a newer
// session that took over the same channel.
type session struct {
	id        uint64
	channelID types.ChannelID
	pc        *webrtc.PeerConnection

	// timeout fires the connect-timeout teardown. It is created (but not yet
	// started) when the session is built, then armed via Reset after the session
	// is registered, so there is never a concurrent read/write of the field.
	// Stopping it during teardown guarantees no timeout goroutine outlives the
	// session.
	timeout *time.Timer

	// endOnce makes the full teardown (close PeerConnection, remove the
	// registration, stop the control stream) reentrant: DELETE, the connection
	// state callback and the timeout goroutine can all fire concurrently while
	// the external stop request is still sent at most once.
	endOnce sync.Once
}

type Source struct {
	log     logrus.FieldLogger
	control *control.Control

	// sessionsMutex guards sessions. It serializes the decision of which single
	// request wins a channel, so two requests passing authentication at the same
	// time can never both register a valid publishing session.
	sessionsMutex sync.RWMutex
	sessions      map[types.ChannelID]*session

	// sessionCounter hands out unique, monotonically increasing session IDs.
	sessionCounter uint64

	// peerConnectionTimeout is the connect timeout. It is a field (rather than
	// only the package constant) so tests can shorten it.
	peerConnectionTimeout time.Duration

	// Listen address of the FS server in the ip:port format
	Address   string
	VideoFile string `mapstructure:"video_file"`
	AudioFile string `mapstructure:"audio_file"`
}

func New(address, videoFile, audioFile string) *Source {
	return &Source{
		Address:               address,
		VideoFile:             videoFile,
		AudioFile:             audioFile,
		sessions:              make(map[types.ChannelID]*session),
		peerConnectionTimeout: PC_TIMEOUT,
	}
}

func (s *Source) SetControl(ctrl *control.Control) {
	s.control = ctrl
}

func (s *Source) SetLogger(log logrus.FieldLogger) {
	s.log = log
}

func (s *Source) Listen(ctx context.Context) {
	s.log.Infof("Registering WHIP http endpoints")

	s.control.RegisterHandleFunc("/whip/endpoint/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Access-Control-Allow-Origin", "*")

		// This function allows for the channel ID to be passed in via the URL /whip/endpoint/1234
		// or alternatively via the stream key 1234-somekey

		strChannelID := path.Base(r.URL.Path)
		streamKey := r.Header.Get("Authorization")
		if streamKey == "" {
			errUnauthorized(w, r)
			return
		}

		if strings.HasPrefix(streamKey, "Bearer ") {
			// Remove Bearer info, will need to research why this is being sent.
			streamKey = strings.Replace(streamKey, "Bearer ", "", 1)
		}

		split := strings.Split(streamKey, "-")
		if len(split) > 1 {
			// Filter out the Channel ID prefix from the stream key
			strChannelID = split[0]
			streamKey = split[1]
		}
		intChannelID, err := strconv.Atoi(strChannelID)
		if err != nil {
			// Illegal channel number: reject before any state is touched.
			errWrongParams(w, r)
			return
		}
		channelID := types.ChannelID(intChannelID)

		if r.Method == http.MethodDelete {
			// Deleting a stream is an authenticated operation; a failed auth must
			// not change any channel state.
			if err = s.control.Authenticate(channelID, types.StreamKey(streamKey)); err != nil {
				errUnauthorized(w, r)
				return
			}

			// End only the session that currently owns the channel. If there is no
			// session (idempotent retry) there is nothing to stop and we must not
			// send a stop request for a stream we do not own.
			if sess := s.lookupSession(channelID); sess != nil {
				s.endSession(sess)
			}

			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "")
			return
		}

		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST, DELETE")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Authentication happens before any session or stream is created, so a
		// rejected request leaves no state behind.
		if err = s.control.Authenticate(channelID, types.StreamKey(streamKey)); err != nil {
			errUnauthorized(w, r)
			return
		}

		offer, err := io.ReadAll(r.Body)
		if err != nil || len(offer) == 0 {
			s.log.Error(err)
			// Empty/invalid body: reject without creating a session or a stream.
			errWrongParams(w, r)
			return
		}

		// Build the PeerConnection fully before registering anything, so a failure
		// here cannot leave a dangling PeerConnection or registration.
		peerConnection, err := webrtc.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			s.log.Error(err)
			errCustom(w, r, "Problem creating the peer connection")
			return
		}

		sess := &session{
			id:        atomic.AddUint64(&s.sessionCounter, 1),
			channelID: channelID,
			pc:        peerConnection,
		}
		// Create the timer in a stopped state; armTimeout starts it only after the
		// session is registered. This keeps the field's lifecycle single-threaded.
		sess.timeout = time.AfterFunc(s.peerConnectionTimeout, func() {
			// Only act while this exact session is still the registered owner.
			if s.lookupSession(sess.channelID) != sess {
				return
			}
			if sess.pc.ConnectionState() != webrtc.PeerConnectionStateConnected {
				s.log.Infof("Peer for channel %s took too long to connect, rejecting peer.", sess.channelID)
				s.endSession(sess)
			}
		})
		sess.timeout.Stop()

		peerConnection.OnICEConnectionStateChange(func(i webrtc.ICEConnectionState) {
			if i == webrtc.ICEConnectionStateFailed {
				if closeErr := peerConnection.Close(); closeErr != nil {
					s.log.Info(closeErr)
				}
			}
		})

		peerConnection.OnConnectionStateChange(func(pcs webrtc.PeerConnectionState) {
			switch pcs {
			case webrtc.PeerConnectionStateClosed,
				webrtc.PeerConnectionStateDisconnected,
				webrtc.PeerConnectionStateFailed:
				// The callback closes over this concrete session, so an old
				// connection dying can never tear down a session that replaced it.
				s.endSession(sess)
			}
		})

		// Atomically claim the channel. Two concurrent requests are serialized
		// here: exactly one registers and the other receives an explicit failure,
		// with nothing left behind.
		if conflict := s.registerSession(sess); conflict != nil {
			// Never take over the winning session; just release our own resources.
			if closeErr := peerConnection.Close(); closeErr != nil {
				s.log.Info(closeErr)
			}
			s.log.WithFields(logrus.Fields{
				"channel_id":       channelID,
				"rejected_session": sess.id,
				"active_session":   conflict.id,
			}).Warn("rejecting overlapping WHIP publish, channel already has an active session")
			errConflict(w, r)
			return
		}

		// From here on every error path ends the registered session, which rolls
		// back the registration and stops the control stream exactly once.
		stream, err := s.control.StartStream(channelID)
		if err != nil {
			s.log.Error(err)
			s.endSession(sess)
			errCustom(w, r, "Problem starting the stream")
			return
		}

		videoTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264}, "video", "pion")
		if err != nil {
			s.log.Error(err)
			s.endSession(sess)
			return
		}

		audioTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "pion")
		if err != nil {
			s.log.Error(err)
			s.endSession(sess)
			return
		}

		stream.AddTrack(videoTrack, webrtc.MimeTypeH264)
		stream.AddTrack(audioTrack, webrtc.MimeTypeOpus)

		stream.ReportMetadata(
			control.AudioCodecMetadata(webrtc.MimeTypeOpus),
			control.VideoCodecMetadata(webrtc.MimeTypeH264),
			control.ClientVendorNameMetadata("waveguide-whip-input"),
			control.ClientVendorVersionMetadata("0.0.1"),
		)

		if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {

			s.log.Error(err)
			s.endSession(sess)
			return
		} else if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {

			s.log.Error(err)
			s.endSession(sess)
			return
		}

		peerConnection.OnTrack(func(remoteTrack *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			codec := remoteTrack.Codec()

			if codec.MimeType == webrtc.MimeTypeOpus {
				s.log.Info("Got Opus track, sending to audio track")
				for {
					if ctx.Err() != nil || stream.Stopped() {
						return
					}

					p, _, err := remoteTrack.ReadRTP()
					if err != nil {
						s.log.Error(err)
						return
					}
					audioTrack.WriteRTP(p)
					stream.ReportMetadata(control.AudioPacketsMetadata(1))
				}
			} else if codec.MimeType == webrtc.MimeTypeH264 {
				s.log.Info("Got H264 track, sending to video track")
				for {
					if ctx.Err() != nil || stream.Stopped() {
						return
					}

					p, _, err := remoteTrack.ReadRTP()
					if err != nil {
						s.log.Error(err)
						return
					}
					videoTrack.WriteRTP(p)
					stream.ReportMetadata(control.VideoPacketsMetadata(1))
				}
			}
		})

		s.armTimeout(sess)

		if err := peerConnection.SetRemoteDescription(webrtc.SessionDescription{
			SDP:  string(offer),
			Type: webrtc.SDPTypeOffer,
		}); err != nil {
			s.log.Error(err)
			s.endSession(sess)
			errWrongParams(w, r)
			return
		}

		gatherComplete := webrtc.GatheringCompletePromise(peerConnection)
		answer, err := peerConnection.CreateAnswer(nil)
		if err != nil {
			s.log.Error(err)
			s.endSession(sess)
			errWrongParams(w, r)
			return
		} else if err = peerConnection.SetLocalDescription(answer); err != nil {
			s.log.Error(err)
			s.endSession(sess)
			errWrongParams(w, r)
			return
		}

		<-gatherComplete

		ttl := time.Now().Add(s.peerConnectionTimeout)

		w.Header().Add("Access-Control-Expose-Headers", "expire")
		w.Header().Add("Content-Type", "application/sdp")
		w.Header().Add("Expire", ttl.Format(http.TimeFormat))
		w.WriteHeader(http.StatusCreated)

		fmt.Fprint(w, peerConnection.LocalDescription().SDP)
	})
}

// registerSession claims the channel for sess while holding the sessions lock.
// It returns the currently active session (without modifying anything) when the
// channel is already owned; otherwise it registers sess and returns nil.
func (s *Source) registerSession(sess *session) *session {
	s.sessionsMutex.Lock()
	defer s.sessionsMutex.Unlock()

	if active, ok := s.sessions[sess.channelID]; ok {
		return active
	}
	s.sessions[sess.channelID] = sess
	return nil
}

// lookupSession returns the session currently registered for the channel, or
// nil when the channel is idle.
func (s *Source) lookupSession(channelID types.ChannelID) *session {
	s.sessionsMutex.RLock()
	defer s.sessionsMutex.RUnlock()

	return s.sessions[channelID]
}

// activeSessionCount returns how many channels currently have a session. It is
// used by tests to assert no registration entries leak.
func (s *Source) activeSessionCount() int {
	s.sessionsMutex.RLock()
	defer s.sessionsMutex.RUnlock()

	return len(s.sessions)
}

// armTimeout starts (or restarts) the connect-timeout timer. The timer itself
// is created synchronously in the request handler, so this never races with
// endSession reading sess.timeout.
func (s *Source) armTimeout(sess *session) {
	sess.timeout.Reset(s.peerConnectionTimeout)
}

// endSession performs the full, reentrant teardown of a session. It is the
// single funnel for DELETE, connection-state callbacks and the timeout. The
// external control stop request runs at most once per session thanks to
// endOnce, and the registry entry is removed only if it still points at this
// session, so an old session can never unregister the new one.
func (s *Source) endSession(sess *session) {
	sess.endOnce.Do(func() {
		if sess.timeout != nil {
			sess.timeout.Stop()
		}

		if closeErr := sess.pc.Close(); closeErr != nil {
			s.log.Info(closeErr)
		}

		owned := false
		s.sessionsMutex.Lock()
		// Only remove the entry if we still own it; a takeover session must survive.
		if current, ok := s.sessions[sess.channelID]; ok && current == sess {
			delete(s.sessions, sess.channelID)
			owned = true
		}
		s.sessionsMutex.Unlock()

		if owned {
			// StopStream is itself idempotent: concurrent callers for the same
			// channel result in a single external stop request.
			if err := s.control.StopStream(sess.channelID); err != nil {
				s.log.Error(err)
			}
		} else {
			s.log.WithFields(logrus.Fields{
				"channel_id": sess.channelID,
				"session":    sess.id,
			}).Info("skipping stop for superseded WHIP session")
		}
	})
}

func errCustom(w http.ResponseWriter, r *http.Request, message string) {
	w.WriteHeader(http.StatusBadRequest)
	w.Header().Set("Content-Type", "plain/text")
	w.Write([]byte(message))
}
func errUnauthorized(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusUnauthorized)
	w.Header().Set("Content-Type", "plain/text")
	w.Write([]byte("Unauthorized"))
}
func errWrongParams(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusBadRequest)
	w.Header().Set("Content-Type", "plain/text")
	w.Write([]byte("Invalid Parameters"))
}

// errConflict rejects a second concurrent publish attempt for a channel that
// already holds an active session.
func errConflict(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusConflict)
	w.Header().Set("Content-Type", "plain/text")
	w.Write([]byte("Channel already has an active publishing session"))
}
