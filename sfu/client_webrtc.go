package sfu

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"atomirex.com/umbrella/razor"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"
)

// errRelayStopped is returned by the dial hook when the relay is removed while
// a connection attempt is still in flight.
var errRelayStopped = fmt.Errorf("relay stopped while establishing")

type clientCommand int

const (
	clientSendProto clientCommand = iota
	clientHandleWSMessage
	clientStop
	clientAddOutgoingTrackForIncomingTrack
	clientRemoveOutgoingTracksForIncomingTrack
	clientKeyframeTick
	clientKeyframeGateUnlock
	clientEvalState
	clientDialWs
	clientIncomingTrackAdded
	clientGetStatus
)

type rawIncomingTrack struct {
	track    *webrtc.TrackRemote
	receiver *webrtc.RTPReceiver
}

type clientCommandMessage struct {
	message          *RemoteNodeMessage
	incomingTrack    *incomingTrack
	newincomingTrack *rawIncomingTrack
	result           *clientCommandResult
}

type clientCommandResult struct {
	status chan *SFUStatusClient
}

type client struct {
	BaseClient

	trunkurl  string // Empty means this is a normal web client - should replace with proper roles or similar
	incoming  *PeerConnection
	outgoing  *PeerConnection
	websocket *websocket.Conn

	// The umbrellaId -> incomingTrack
	incomingTracks map[string]*incomingTrackWithClientState

	// The umbrellaId -> outgoingTrack
	outgoingTracks map[string]*outgoingTrackWithClientState

	// The umbrellaId -> RTPSender
	senders map[string]*webrtc.RTPSender

	handler *razor.MessageHandler[clientCommand, clientCommandMessage]

	// mid <-> umbrella track ID mappings - only set when known to be valid!
	incomingMidToUmbrellaTrackID map[string]string

	// Incoming tracks that have yet to be attached to MIDs
	stagedIncomingTracks []*rawIncomingTrack

	dialMu     sync.Mutex
	dialCancel context.CancelFunc
	dialConn   net.Conn

	// wsMu guards the websocket pointer only (never a network operation), so
	// stop() can always take it and close the connection, which in turn
	// unblocks any writer waiting in writeMu.
	wsMu    sync.Mutex
	writeMu sync.Mutex
}

func (c *client) currentWebsocket() *websocket.Conn {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.websocket
}

// trackDialConn records the raw connection used for an in-flight dial so that
// stop() can abort a handshake which is still waiting for a response.
func (c *client) trackDialConn(conn net.Conn) bool {
	c.dialMu.Lock()
	defer c.dialMu.Unlock()

	if c.stopped.Load() {
		return false
	}

	c.dialConn = conn
	return true
}

func (c *client) forgetDialConn(conn net.Conn) {
	c.dialMu.Lock()
	if c.dialConn == conn {
		c.dialConn = nil
	}
	c.dialMu.Unlock()
}

// closeWebsocket closes the websocket once, unblocking any pending reader.
func (c *client) closeWebsocket() {
	c.wsMu.Lock()
	ws := c.websocket
	c.websocket = nil
	c.wsMu.Unlock()

	if ws != nil {
		ws.Close()
	}
}

func (c *client) getStatus() *SFUStatusClient {
	msg := clientCommandMessage{result: &clientCommandResult{
		status: make(chan *SFUStatusClient, 1),
	}}

	// If the client handler has already been aborted it will never reply, so
	// never block the caller (the SFU command loop) indefinitely.
	if !c.handler.Send(clientGetStatus, &msg) {
		return nil
	}

	select {
	case status := <-msg.result.status:
		return status
	case <-time.After(statusQueryTimeout):
		c.logger.Error(c.label, "Timed out waiting for client status")
		return nil
	}
}

func (c *client) run(ws *websocket.Conn, s *Sfu) {
	c.incomingTracks = make(map[string]*incomingTrackWithClientState)
	c.outgoingTracks = make(map[string]*outgoingTrackWithClientState)

	c.incomingMidToUmbrellaTrackID = make(map[string]string)

	c.stagedIncomingTracks = make([]*rawIncomingTrack, 0)

	c.senders = make(map[string]*webrtc.RTPSender)

	incoming, err := s.peerConnectionFactory.NewPeerConnection(fmt.Sprintf("incoming for %s", c.label))
	if c.logger.NilErrCheck(c.label, "Failed to create an incoming peer connection", err) {
		return
	}

	outgoing, err := s.peerConnectionFactory.NewPeerConnection(fmt.Sprintf("outgoing for %s", c.label))
	if c.logger.NilErrCheck(c.label, "Failed to create an outgoing peer connection", err) {
		return
	}

	c.incoming = incoming
	c.outgoing = outgoing

	falseptr := false

	_, err = incoming.CreateDataChannel("data-out", &webrtc.DataChannelInit{Ordered: &falseptr})
	c.logger.NilErrCheck(c.label, "Failed to create an incoming data channel", err)

	_, err = outgoing.CreateDataChannel("data-in", &webrtc.DataChannelInit{Ordered: &falseptr})
	c.logger.NilErrCheck(c.label, "Failed to create an outgoing data channel", err)

	_, err = c.outgoing.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	c.logger.NilErrCheck(c.label, "Error adding fake transceiver to outgoing pc", err)

	// keyframe throttling
	sendKeyFrameGate := false

	c.handler = razor.NewMessageHandler(c.logger, c.label, 1024, func(what clientCommand, payload *clientCommandMessage) bool {
		shouldSendKeyframe := false
		shouldEvalState := false

		stop := func() {
			c.logger.Info(c.label, "Stopping")

			c.handler.Abort()

			c.logger.Info(c.label, "Stopped")
		}

		switch what {
		case clientKeyframeTick:
			shouldSendKeyframe = true
		case clientKeyframeGateUnlock:
			sendKeyFrameGate = false
		case clientSendProto:
			ws := c.currentWebsocket()

			if ws == nil {
				c.logger.Error(c.label, "Attempting send when not connected to websocket")
				return true
			}

			data, err := proto.Marshal(payload.message)

			if err != nil {
				c.logger.Error(c.label, "Failed to marshal proto for client")
				stop()
				return true
			}

			c.logger.Verbose(c.label, "ws proto sending "+payload.message.String())

			// Serialize writes to the websocket only. The pointer lock is never
			// held across a network operation so stop() can close the
			// connection and unblock this write.
			c.writeMu.Lock()
			err = ws.WriteMessage(websocket.BinaryMessage, data)
			c.writeMu.Unlock()

			if c.logger.NilErrCheck(c.label, "Failed to write proto to ws", err) {
				stop()
				return true
			}
		case clientHandleWSMessage:
			c.handleWsMessage(payload.message, s)
		case clientStop:
			stop()
			return true
		case clientAddOutgoingTrackForIncomingTrack:
			// Add it to our outgoing if it's not on incoming
			if _, incomingExists := c.incomingTracks[payload.incomingTrack.UmbrellaID()]; !incomingExists {
				c.outgoingTracks[payload.incomingTrack.UmbrellaID()] = &outgoingTrackWithClientState{
					track:  &outgoingTrack{descriptor: payload.incomingTrack.descriptor},
					source: payload.incomingTrack,
				}

				c.handler.Cancel(clientEvalState)
				c.handler.Timeout(clientEvalState, nil, 500*time.Millisecond)
			}
		case clientRemoveOutgoingTracksForIncomingTrack:
			if _, exists := c.outgoingTracks[payload.incomingTrack.UmbrellaID()]; exists {
				delete(c.outgoingTracks, payload.incomingTrack.UmbrellaID())

				c.handler.Cancel(clientEvalState)
				c.handler.Timeout(clientEvalState, nil, 500*time.Millisecond)
			}
		case clientEvalState:
			if c.pcTerminated() {
				stop()
				return true
			}

			shouldEvalState = true
		case clientGetStatus:
			// Like the SFU this is horribly blocking
			// Also could cause problems if the client goes AWOL while the SFU is wanting to get status (i.e. would need a timeout)

			intd := make([]*TrackDescriptor, 0)
			for _, t := range c.incomingTracks {
				intd = append(intd, t.track.descriptor)
			}
			outtd := make([]*TrackDescriptor, 0)
			for _, t := range c.outgoingTracks {
				outtd = append(outtd, t.track.descriptor)
			}
			senderStatus := make([]*SFUStatusSender, 0)
			for umbrellaId, s := range c.senders {
				t := s.Track()
				id := ""
				if t != nil {
					id = t.ID()
				}
				senderStatus = append(senderStatus, &SFUStatusSender{HasTrack: t != nil, TrackIdIfSet: id, UmbrellaId: umbrellaId})
			}
			midMapping := make([]*MidToUmbrellaIDMapping, 0)
			for mid, umbrellaId := range c.incomingMidToUmbrellaTrackID {
				midMapping = append(midMapping, &MidToUmbrellaIDMapping{Mid: mid, UmbrellaId: umbrellaId})
			}
			stagedIncoming := make([]*SFUStatusStagedIncomingTrack, 0)
			for _, s := range c.stagedIncomingTracks {
				stagedIncoming = append(stagedIncoming, &SFUStatusStagedIncomingTrack{
					StreamId: s.track.StreamID(),
					TrackId:  s.track.ID(),
					Mid:      s.receiver.RTPTransceiver().Mid(),
				})
			}

			status := &SFUStatusClient{
				Label:                c.label,
				TrunkUrl:             c.trunkurl,
				IncomingPC:           c.incoming.GetStatus(),
				OutgoingPC:           c.outgoing.GetStatus(),
				IncomingTracks:       intd,
				OutgoingTracks:       outtd,
				Senders:              senderStatus,
				MidMapping:           midMapping,
				StagedIncomingTracks: stagedIncoming,
			}

			payload.result.status <- status
		case clientDialWs:
			if c.currentWebsocket() != nil {
				return true
			}

			if c.stopped.Load() {
				return true
			}

			c.dialMu.Lock()
			dialCtx, cancel := context.WithCancel(context.Background())
			c.dialCancel = cancel
			c.dialMu.Unlock()

			dialer := websocket.Dialer{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // TODO remove this bad ignoring of certs, but useful for local testing
			}

			dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					port = "443" // Default wss port
				} else if strings.HasSuffix(host, ".local") {
					// Use our own mdns to discover it, if possible
					// This is to enable an OpenWrt AP to discover Apple servers on the same subnet
					found, err := s.mdnsLookup(host)
					if err == nil {
						host = found
					}
				}

				netDialer := &net.Dialer{}
				conn, err := netDialer.DialContext(ctx, network, net.JoinHostPort(host, port))
				if err != nil {
					return nil, err
				}

				if !c.trackDialConn(conn) {
					conn.Close()
					return nil, errRelayStopped
				}

				return conn, nil
			}

			conn, _, err := dialer.DialContext(dialCtx, c.trunkurl, nil)
			cancel()

			if conn != nil {
				c.forgetDialConn(conn.UnderlyingConn())
			}

			if c.stopped.Load() {
				// We were removed while dialling: do not leave a half connected
				// instance behind, and do not report a failure for a relay which
				// was intentionally stopped.
				if conn != nil {
					conn.Close()
				}
				return true
			}

			if c.logger.NilErrCheck(c.label, "Error dialling ws "+c.trunkurl, err) {
				// Establishment failed: tell the SFU so it drops this relay and
				// allows a later submission of the same address to retry.
				c.notifyFailed()
				stop()
				return true
			}

			c.wsMu.Lock()
			c.websocket = conn
			c.wsMu.Unlock()

			go func() {
				c.continueWebsocket(s)
			}()
		case clientIncomingTrackAdded:
			t := payload.newincomingTrack.track
			tsc := payload.newincomingTrack.receiver.RTPTransceiver()

			c.logger.Info(c.label, "Incoming track to add stream id "+t.StreamID()+" "+t.ID()+" MID "+tsc.Mid())

			c.stagedIncomingTracks = append(c.stagedIncomingTracks, payload.newincomingTrack)

			c.evalIncomingState(s)
		}

		if shouldEvalState {
			c.handler.Cancel(clientEvalState)

			shouldSendKeyframe = true
			c.evalState(s)
		}

		if shouldSendKeyframe {
			// Throttled
			if !sendKeyFrameGate {
				c.handler.Cancel(clientKeyframeTick)

				sendKeyFrameGate = true
				c.handler.Timeout(clientKeyframeGateUnlock, nil, time.Millisecond*500)

				for _, receiver := range c.incoming.GetReceivers() {
					if receiver.Track() != nil {
						_ = c.incoming.WriteRTCP([]rtcp.Packet{
							&rtcp.PictureLossIndication{
								MediaSSRC: uint32(receiver.Track().SSRC()),
							},
						})
					}
				}

				c.handler.Timeout(clientKeyframeTick, nil, time.Second*3)
			}
		}

		return true
	})

	if ws == nil {
		c.handler.Send(clientDialWs, nil)
	}

	c.handler.Loop(func() {
		for _, it := range c.incomingTracks {
			s.removeOutgoingTracksForIncomingTrack(it.track)
		}

		c.closeWebsocket()

		// Use an immediate close rather than a graceful one: this peer is being
		// torn down, and a graceful close can take seconds, which would delay a
		// replacement relay for the same address and make the server list lag
		// behind the submitted intent.
		if outgoing != nil {
			outgoing.Close()
		}
		outgoing = nil

		if incoming != nil {
			incoming.Close()
		}
		incoming = nil

		c.logger.Info(c.label, "Post clean up finished")

		// Tell the SFU this relay instance is fully torn down. This is the point
		// at which a replacement instance for the same address may be created.
		c.notifyTerminated()
	})
}

func (c *client) stop() {
	c.stopped.Store(true)

	// Unblock an in-flight dial so a relay removed mid-establishment cannot
	// linger as a half connected instance. Cancelling the context aborts a
	// connection attempt, and closing the tracked connection aborts a
	// handshake which is still waiting for bytes from the peer.
	c.dialMu.Lock()
	if c.dialCancel != nil {
		c.dialCancel()
		c.dialCancel = nil
	}
	if c.dialConn != nil {
		c.dialConn.Close()
		c.dialConn = nil
	}
	c.dialMu.Unlock()

	// Unblock the websocket reader so a stopped relay tears down promptly.
	c.closeWebsocket()

	c.handler.CancelAll()
	c.handler.Send(clientStop, nil)
}

func (c *client) pcTerminated() bool {
	return c.incoming.IsTerminated() || c.outgoing.IsTerminated()
}

func (c *client) handleWsMessage(message *RemoteNodeMessage, s *Sfu) {
	if message.Candidate != nil {
		c.logger.Info(c.label, "WS PROTO RECEIVED ice candidate")
		candidate := webrtc.ICECandidateInit{}
		if err := json.Unmarshal([]byte(message.Candidate.Candidate), &candidate); err != nil {
			c.logger.Error(c.label, "Failed to unmarshal json to candidate: "+err.Error())
			return
		}

		c.logger.Info(c.label, "Got candidate: "+candidate.Candidate)

		pc := c.incoming
		// Incoming from pov of the sender!
		if message.Candidate.Incoming {
			pc = c.outgoing
		}

		if err := pc.AddICECandidate(candidate); err != nil {
			c.logger.Error(c.label, "Failed to add ICE candidate: "+err.Error())
			return
		}
	}

	if message.Answer != nil {
		c.logger.Info(c.label, "WS PROTO RECEIVED answer")
		answer := webrtc.SessionDescription{}
		if err := json.Unmarshal([]byte(message.Answer.Answer), &answer); err != nil {
			c.logger.Error(c.label, "Failed to umarshal JSON to answer: "+err.Error())
			return
		}

		c.logger.Info(c.label, "Got answer: "+answer.SDP)

		if err := c.outgoing.SetRemoteDescription(answer); err != nil {
			c.logger.Error(c.label, "Failed to set remote description on outgoing from answer: "+err.Error())
			return
		}
	}

	if message.Offer != nil {
		c.logger.Info(c.label, "WS PROTO RECEIVED offer")
		offer := webrtc.SessionDescription{}
		if err := json.Unmarshal([]byte(message.Offer.Offer), &offer); err != nil {
			c.logger.Error(c.label, "Failed to umarshal JSON to offer: "+err.Error())
			return
		}

		c.logger.Info(c.label, "Got offer: "+offer.SDP)

		if err := c.incoming.SetRemoteDescription(offer); err != nil {
			c.logger.Error(c.label, "Failed to set remote description on incoming from offer: "+err.Error())
			return
		}

		answer, err := c.incoming.CreateAnswer(&webrtc.AnswerOptions{})
		if err != nil {
			c.logger.Error(c.label, "Failed to create answer in response to offer: "+err.Error())
			return
		}

		c.incoming.SetLocalDescription(answer)

		answerString, err := json.Marshal(answer)
		if err != nil {
			c.logger.Error(c.label, "Failed to marshal answer to json: "+err.Error())
			return
		}

		c.writeProto(&RemoteNodeMessage{Answer: &AnswerMessage{Answer: string(answerString)}})
	}

	if message.AcceptTracks != nil {
		c.logger.Info(c.label, "WS PROTO RECEIVED accept tracks "+message.AcceptTracks.String())

		// Mark the accepted tracks and schedule an evaluation
		for _, td := range message.AcceptTracks.Tracks {
			ot := c.outgoingTracks[td.UmbrellaId]
			if ot != nil {
				ot.remoteNotified = true
				ot.remoteAccepted = true
			}
		}

		c.handler.Send(clientEvalState, nil)
	}

	if message.UpstreamTracks != nil {
		c.logger.Info(c.label, "WS PROTO RECEIVED upstream tracks "+message.UpstreamTracks.String())

		// Any previously unknown upstream tracks need to have transceivers created for them
		// Then we acknowledge with the complete set of expected upstream tracks
		for _, td := range message.UpstreamTracks.Tracks {
			_, exists := c.incomingTracks[td.UmbrellaId]
			if !exists {
				if td.Kind != TrackKind_Unknown {
					c.logger.Info(c.label, "Adding transceiver "+td.Id+" "+td.Kind.String())

					_, err := c.incoming.AddTransceiverFromKind(trackKindToWebrtcKind(td.Kind), webrtc.RTPTransceiverInit{
						Direction: webrtc.RTPTransceiverDirectionRecvonly,
					})

					if err != nil {
						c.logger.Error(c.label, "Failed to add transceiver "+err.Error())
					} else {
						intrack := &incomingTrackWithClientState{
							track: &incomingTrack{descriptor: td, owner: c},
						}

						c.incomingTracks[intrack.UmbrellaID()] = intrack
					}
				}
			}
		}

		acceptedTracks := make([]*TrackDescriptor, 0)
		for _, intrack := range c.incomingTracks {
			acceptedTracks = append(acceptedTracks, intrack.track.descriptor)
		}

		c.writeProto(&RemoteNodeMessage{AcceptTracks: &AcceptUpstreamTracks{Tracks: acceptedTracks}})

		s.handler.Send(sfuSignalClients, nil)
	}

	if message.MidMappings != nil {
		c.logger.Info(c.label, "WS PROTO RECEIVED mid <-> umbrella mapping "+message.MidMappings.String())
		// Review all incoming tracks to assign MIDs, and if newly so then fan out appropriately

		// Update all the mappings in the client
		for _, mapping := range message.MidMappings.Mapping {
			c.incomingMidToUmbrellaTrackID[mapping.Mid] = mapping.UmbrellaId
		}

		c.evalIncomingState(s)
	}
}

func (c *client) evalIncomingState(s *Sfu) {
	c.logger.Info(c.label, "Eval incoming state")

	// Review staged incoming tracks to see if any can be updated as a result
	for i := 0; i < len(c.stagedIncomingTracks); i++ {
		sit := c.stagedIncomingTracks[i]

		mid := sit.receiver.RTPTransceiver().Mid()
		if mid == "" {
			continue
		}

		umbrellaId, midKnown := c.incomingMidToUmbrellaTrackID[mid]
		if midKnown {
			intrack, trackExists := c.incomingTracks[umbrellaId]
			if trackExists {
				if intrack.track.remote == nil {
					c.logger.Info(c.label, "Ready to fan out track: "+umbrellaId)

					// Assign it, remove from staged, and launch fan out
					intrack.track.remote = sit.track

					intrack.track.descriptor.Id = sit.track.ID()
					intrack.track.descriptor.StreamId = sit.track.StreamID()
					intrack.track.receiver = sit.receiver
					intrack.track.owner = c
					intrack.transceiverMid = mid

					// Remove from staged
					c.stagedIncomingTracks = append(c.stagedIncomingTracks[:i], c.stagedIncomingTracks[i+1:]...)
					i--

					relay, err := webrtc.NewTrackLocalStaticRTP(intrack.track.remote.Codec().RTPCodecCapability, "UMB_RELAY"+uuid.New().String(), intrack.track.remote.StreamID())
					if err != nil {
						panic(err)
					}

					intrack.track.relay = relay

					go c.fanoutIncoming(intrack.track, s)

					s.handler.Send(sfuAddOutgoingTracksForIncomingTrack, &sfuCommandMessage{intrack: intrack.track})
					c.logger.Info(c.label, "New incoming track sent to SFU: "+umbrellaId)
				}
			}
		}
	}
}

func (c *client) fanoutIncoming(intrack *incomingTrack, s *Sfu) {
	defer s.removeOutgoingTracksForIncomingTrack(intrack)

	bufSize := 32768

	if intrack.remote.Kind() == webrtc.RTPCodecTypeVideo {
		bufSize = bufSize * 8
	}

	buf := make([]byte, bufSize)

	rtpPkt := &rtp.Packet{}
	for {
		i, _, err := intrack.remote.Read(buf)
		if c.logger.NilErrCheck(c.label, "Fan out error reading rtp on track "+intrack.String(), err) {
			return
		}

		if err = rtpPkt.Unmarshal(buf[:i]); err != nil {
			c.logger.Error(c.label, "Error unmarshaling rtp on track "+intrack.String()+" "+err.Error())
			return
		}

		rtpPkt.Extension = false
		rtpPkt.Extensions = nil

		if err := intrack.relay.WriteRTP(rtpPkt); err != nil {
			c.logger.Error(c.label, "Error writing rtp from "+intrack.String()+" to relay "+err.Error())
			return
		}
	}
}

func (c *client) evalState(s *Sfu) {
	// Do we have any non attached tracks we should be uploading which have not yet been sent for confirmation?
	needsNotification := false
	for _, ot := range c.outgoingTracks {
		needsNotification = needsNotification || !ot.remoteNotified
	}

	// If so, send them up, return and wait before evalling again
	if needsNotification {
		notifications := make([]*TrackDescriptor, 0)
		for _, ot := range c.outgoingTracks {
			notifications = append(notifications, ot.track.descriptor)
			ot.remoteNotified = true
		}

		c.writeProto(&RemoteNodeMessage{UpstreamTracks: &SetUpstreamTracks{Tracks: notifications}})

		c.handler.Cancel(clientEvalState)
		c.handler.Timeout(clientEvalState, nil, 300*time.Millisecond) // Should probably do this a better way, like in the event listener
		return
	}

	// If we have any we're waiting on confirmation of we should return and wait before evalling again
	needsConfirmation := false
	for _, ot := range c.outgoingTracks {
		needsConfirmation = needsConfirmation || !ot.remoteAccepted
	}

	if needsConfirmation {
		// Wait for the accept tracks event

		c.handler.Cancel(clientEvalState)
		c.handler.Timeout(clientEvalState, nil, 300*time.Millisecond) // Should probably do this a better way, like in the event listener
		return
	}

	ss := c.outgoing.wrapped.SignalingState()
	if ss != webrtc.SignalingStateStable {
		c.handler.Cancel(clientEvalState)
		c.handler.Timeout(clientEvalState, nil, 300*time.Millisecond) // Should probably do this a better way, like in the event listener
		return
	}

	senderRemovalFailed := false
	// Find any senders that don't have a track, and remove them
	for umbrellaId, sender := range c.senders {
		_, exists := c.outgoingTracks[umbrellaId]
		if !exists {
			c.logger.Debug(c.label, "eval state removing sender for track with umb id "+umbrellaId)

			if err := c.outgoing.RemoveTrack(sender); err != nil {
				c.logger.Error(c.label, "Error removing sender for track with umb id "+umbrellaId+" "+err.Error())
				senderRemovalFailed = true
			} else {
				delete(c.senders, umbrellaId)
			}
		}
	}

	if senderRemovalFailed {
		// Retry again later
		c.handler.Cancel(clientEvalState)
		c.handler.Timeout(clientEvalState, nil, 300*time.Millisecond)
		return
	}

	addingTrackFailed := false
	// Find any tracks which don't have a sender, and add them
	for umbrellaId, ot := range c.outgoingTracks {
		sender, exists := c.senders[umbrellaId]
		if !exists || sender.Track() == nil {
			c.logger.Debug(c.label, "eval state creating sender for track with umb id "+umbrellaId)
			if sender, err := c.outgoing.AddTrack(ot.source.relay); err != nil {
				c.logger.Error(c.label, "Error creating sender for track with umb id "+umbrellaId+" "+err.Error())
				addingTrackFailed = true
			} else {
				c.senders[umbrellaId] = sender
			}
		}
	}

	if addingTrackFailed {
		// Retry again later
		c.handler.Cancel(clientEvalState)
		c.handler.Timeout(clientEvalState, nil, 300*time.Millisecond)
		return
	}

	c.logger.Info(c.label, "eval state creating offer")
	offer, err := c.outgoing.CreateOffer(nil)
	if c.logger.NilErrCheck(c.label, "eval state creating offer error", err) {
		// Retry again later
		c.handler.Cancel(clientEvalState)
		c.handler.Timeout(clientEvalState, nil, 300*time.Millisecond)
		c.logger.Info(c.label, "eval state creating offer erred so retry scheduled")
		return
	}

	c.logger.Info(c.label, "eval state setting local description")
	if err = c.outgoing.SetLocalDescription(offer); err != nil {
		c.logger.Error(c.label, "eval state setting local description error"+err.Error())
		// Retry again later
		c.handler.Cancel(clientEvalState)
		c.handler.Timeout(clientEvalState, nil, 300*time.Millisecond)
		c.logger.Info(c.label, "eval state setting local description erred so retry scheduled")
		return
	}

	offerString, err := json.Marshal(offer)
	if c.logger.NilErrCheck(c.label, "Eval state offer marshalling failed", err) {
		return
	}

	c.logger.Info(c.label, "Send offer to client: "+offer.SDP)

	c.writeProto(&RemoteNodeMessage{
		Offer: &OfferMessage{
			Offer: string(offerString),
		},
	})
}

func (c *client) writeProto(m *RemoteNodeMessage) {
	c.handler.Send(clientSendProto, &clientCommandMessage{message: m})
}

func (c *client) continueWebsocket(s *Sfu) {
	// This takes care of the various cleanup
	defer func() {
		c.stop()
	}()

	ws := c.currentWebsocket()

	if ws == nil {
		c.notifyTerminated()
		return
	}

	incoming := c.incoming
	outgoing := c.outgoing

	s.handler.Send(sfuAddClient, &sfuCommandMessage{client: c})

	defer s.handler.Send(sfuRemoveClient, &sfuCommandMessage{client: c})

	icecandidate := func(i *webrtc.ICECandidate, incoming bool) {
		if i == nil {
			return
		}

		// If you are serializing a candidate make sure to use ToJSON
		// Using Marshal will result in errors around `sdpMid` (thanks pion commenter!)
		candidateString, err := json.Marshal(i.ToJSON())

		if c.logger.NilErrCheck(c.label, "Failed to marshal candidate to json", err) {
			return
		}

		c.writeProto(&RemoteNodeMessage{
			Candidate: &CandidateMessage{
				Candidate: string(candidateString),
				Incoming:  incoming,
			},
		})
	}

	incoming.SetOnICECandidate(func(i *webrtc.ICECandidate) {
		icecandidate(i, true)
	})

	outgoing.SetOnICECandidate(func(i *webrtc.ICECandidate) {
		icecandidate(i, false)
	})

	incoming.SetOnConnectionStateChange(func(p webrtc.PeerConnectionState) {
		switch p {
		case webrtc.PeerConnectionStateFailed:
		case webrtc.PeerConnectionStateDisconnected:
		case webrtc.PeerConnectionStateClosed:
			c.stop()
		default:
		}

		s.handler.Send(sfuSignalClients, nil)
	})

	outgoing.SetOnConnectionStateChange(func(p webrtc.PeerConnectionState) {
		switch p {
		case webrtc.PeerConnectionStateFailed:
		case webrtc.PeerConnectionStateDisconnected:
		case webrtc.PeerConnectionStateClosed:
			c.stop()
		default:
		}

		s.handler.Send(sfuSignalClients, nil)
	})

	outgoing.SetOnSignalingStateChange(func(ss webrtc.SignalingState) {
		if ss == webrtc.SignalingStateStable {
			mappings := make([]*MidToUmbrellaIDMapping, 0)

			for umbrellaId := range c.outgoingTracks {
				sender, exists := c.senders[umbrellaId]
				if exists && sender.Track() != nil {
					for _, trx := range outgoing.GetTransceivers() {
						if trx.Sender() == sender {
							mappings = append(mappings, &MidToUmbrellaIDMapping{
								Mid:        trx.Mid(),
								UmbrellaId: umbrellaId,
							})
						}
					}
				}
			}

			c.writeProto(&RemoteNodeMessage{MidMappings: &MidToUmbrellaIDMappings{Mapping: mappings}})
		}
	})

	incoming.SetOnTrack(func(t *webrtc.TrackRemote, r *webrtc.RTPReceiver) {
		c.logger.Info(c.label, "OnTrack "+t.ID()+" "+r.RTPTransceiver().Mid())
		c.handler.Send(clientIncomingTrackAdded, &clientCommandMessage{newincomingTrack: &rawIncomingTrack{track: t, receiver: r}})
	})

	incoming.SetOnNegotiationNeeded(func() {
		c.handler.Send(clientEvalState, nil)
	})

	outgoing.SetOnNegotiationNeeded(func() {
		c.handler.Send(clientEvalState, nil)
	})

	// Signal for the new PeerConnection
	s.handler.Send(sfuSignalClients, nil)

	for {
		var message RemoteNodeMessage
		_, raw, err := ws.ReadMessage()
		if c.logger.NilErrCheck(c.label, "Failed to read message", err) {
			return
		}

		if err := proto.Unmarshal(raw, &message); err != nil {
			c.logger.Error(c.label, "Failed to unmarshal proto to message: "+err.Error())
			return
		}

		c.handler.Send(clientHandleWSMessage, &clientCommandMessage{message: &message})
	}
}

func (c *client) AddOutgoingTracksForIncomingTrack(intrack *incomingTrack) {
	c.handler.Send(clientAddOutgoingTrackForIncomingTrack, &clientCommandMessage{incomingTrack: intrack})
}

func (c *client) RemoveOutgoingTracksForIncomingTrack(intrack *incomingTrack) {
	c.handler.Send(clientRemoveOutgoingTracksForIncomingTrack, &clientCommandMessage{incomingTrack: intrack})
}

func (c *client) RequestEvalState() {
	c.handler.Send(clientEvalState, nil)
}
