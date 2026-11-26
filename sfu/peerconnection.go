package sfu

import (
	"sync"

	"atomirex.com/umbrella/razor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// PeerConnection wraps a Pion peer connection. The On* callbacks are read from
// Pion's own goroutines, so they are stored behind a mutex and must be set via
// the corresponding Set* methods rather than by direct field assignment.
type PeerConnection struct {
	label   string
	wrapped *webrtc.PeerConnection
	logger  *razor.Logger

	callbacksMu sync.RWMutex

	OnICECandidate             func(w *webrtc.ICECandidate)
	OnICEConnectionStateChange func(is webrtc.ICEConnectionState)
	OnSignalingStateChange     func(ss webrtc.SignalingState)
	OnConnectionStateChange    func(pcs webrtc.PeerConnectionState)
	OnTrack                    func(tr *webrtc.TrackRemote, r *webrtc.RTPReceiver)
	OnNegotiationNeeded        func()
}

func (pc *PeerConnection) SetOnICECandidate(fn func(w *webrtc.ICECandidate)) {
	pc.callbacksMu.Lock()
	pc.OnICECandidate = fn
	pc.callbacksMu.Unlock()
}

func (pc *PeerConnection) SetOnICEConnectionStateChange(fn func(is webrtc.ICEConnectionState)) {
	pc.callbacksMu.Lock()
	pc.OnICEConnectionStateChange = fn
	pc.callbacksMu.Unlock()
}

func (pc *PeerConnection) SetOnSignalingStateChange(fn func(ss webrtc.SignalingState)) {
	pc.callbacksMu.Lock()
	pc.OnSignalingStateChange = fn
	pc.callbacksMu.Unlock()
}

func (pc *PeerConnection) SetOnConnectionStateChange(fn func(pcs webrtc.PeerConnectionState)) {
	pc.callbacksMu.Lock()
	pc.OnConnectionStateChange = fn
	pc.callbacksMu.Unlock()
}

func (pc *PeerConnection) SetOnTrack(fn func(tr *webrtc.TrackRemote, r *webrtc.RTPReceiver)) {
	pc.callbacksMu.Lock()
	pc.OnTrack = fn
	pc.callbacksMu.Unlock()
}

func (pc *PeerConnection) SetOnNegotiationNeeded(fn func()) {
	pc.callbacksMu.Lock()
	pc.OnNegotiationNeeded = fn
	pc.callbacksMu.Unlock()
}

func (pc *PeerConnection) iceCandidateCallback() func(w *webrtc.ICECandidate) {
	pc.callbacksMu.RLock()
	defer pc.callbacksMu.RUnlock()
	return pc.OnICECandidate
}

func (pc *PeerConnection) iceConnectionStateChangeCallback() func(is webrtc.ICEConnectionState) {
	pc.callbacksMu.RLock()
	defer pc.callbacksMu.RUnlock()
	return pc.OnICEConnectionStateChange
}

func (pc *PeerConnection) signalingStateChangeCallback() func(ss webrtc.SignalingState) {
	pc.callbacksMu.RLock()
	defer pc.callbacksMu.RUnlock()
	return pc.OnSignalingStateChange
}

func (pc *PeerConnection) connectionStateChangeCallback() func(pcs webrtc.PeerConnectionState) {
	pc.callbacksMu.RLock()
	defer pc.callbacksMu.RUnlock()
	return pc.OnConnectionStateChange
}

func (pc *PeerConnection) trackCallback() func(tr *webrtc.TrackRemote, r *webrtc.RTPReceiver) {
	pc.callbacksMu.RLock()
	defer pc.callbacksMu.RUnlock()
	return pc.OnTrack
}

func (pc *PeerConnection) negotiationNeededCallback() func() {
	pc.callbacksMu.RLock()
	defer pc.callbacksMu.RUnlock()
	return pc.OnNegotiationNeeded
}

type PeerConnectionFactory interface {
	NewPeerConnection(label string) (*PeerConnection, error)
}

type PionPeerConnectionFactory struct {
	webrtcApi *webrtc.API
	pcConfig  *webrtc.Configuration
	logger    *razor.Logger
}

func (p *PionPeerConnectionFactory) NewPeerConnection(label string) (*PeerConnection, error) {
	pc, err := p.webrtcApi.NewPeerConnection(*p.pcConfig)

	if err != nil {
		return nil, err
	}

	newPc := &PeerConnection{
		label:   label,
		wrapped: pc,
		logger:  p.logger,
	}

	pc.OnICECandidate(func(i *webrtc.ICECandidate) {
		p.logger.Verbose(label, "Ice candidate")
		if cb := newPc.iceCandidateCallback(); cb != nil {
			cb(i)
		}
	})

	pc.OnSignalingStateChange(func(ss webrtc.SignalingState) {
		p.logger.Info(label, "OnSignalingStateChange"+ss.String())
		if cb := newPc.signalingStateChangeCallback(); cb != nil {
			cb(ss)
		}
	})

	pc.OnICEConnectionStateChange(func(is webrtc.ICEConnectionState) {
		p.logger.Info(label, "OnICEConnectionStateChange"+is.String())
		if cb := newPc.iceConnectionStateChangeCallback(); cb != nil {
			cb(is)
		}
	})

	pc.OnConnectionStateChange(func(pcs webrtc.PeerConnectionState) {
		p.logger.Info(label, "OnConnectionStateChange"+pcs.String())
		if cb := newPc.connectionStateChangeCallback(); cb != nil {
			cb(pcs)
		}
	})

	pc.OnTrack(func(tr *webrtc.TrackRemote, r *webrtc.RTPReceiver) {
		p.logger.Info(label, "OnTrack")
		if cb := newPc.trackCallback(); cb != nil {
			cb(tr, r)
		}
	})

	pc.OnNegotiationNeeded(func() {
		p.logger.Info(label, "OnNegotiationNeeded")
		if cb := newPc.negotiationNeededCallback(); cb != nil {
			cb()
		}
	})

	return newPc, nil
}

func (pc *PeerConnection) GetStatus() *SFUStatusPeerConnection {
	return &SFUStatusPeerConnection{
		ConnectionState:    pc.wrapped.ConnectionState().String(),
		SignalingState:     pc.wrapped.SignalingState().String(),
		IceConnectionState: pc.wrapped.ICEConnectionState().String(),
		IceGatheringState:  pc.wrapped.ICEGatheringState().String(),

		TransceiverCount: int32(len(pc.wrapped.GetTransceivers())),
		SenderCount:      int32(len(pc.wrapped.GetSenders())),
		ReceiverCount:    int32(len(pc.wrapped.GetReceivers())),
	}
}

func (pc *PeerConnection) Close() error {
	pc.logger.Info(pc.label, "Closing")

	err := pc.wrapped.Close()

	pc.logger.NilErrCheck(pc.label, "Error when closing", err)

	return err
}

func (pc *PeerConnection) CreateDataChannel(name string, params *webrtc.DataChannelInit) (*webrtc.DataChannel, error) {
	return pc.wrapped.CreateDataChannel(name, params)
}

func (pc *PeerConnection) GetReceivers() []*webrtc.RTPReceiver {
	return pc.wrapped.GetReceivers()
}

func (pc *PeerConnection) GetSenders() []*webrtc.RTPSender {
	return pc.wrapped.GetSenders()
}

func (pc *PeerConnection) GetTransceivers() []*webrtc.RTPTransceiver {
	return pc.wrapped.GetTransceivers()
}

func (pc *PeerConnection) WriteRTCP(pkts []rtcp.Packet) error {
	return pc.wrapped.WriteRTCP(pkts)
}

func (pc *PeerConnection) AddTransceiverFromKind(kind webrtc.RTPCodecType, params webrtc.RTPTransceiverInit) (*webrtc.RTPTransceiver, error) {
	return pc.wrapped.AddTransceiverFromKind(kind, params)
}

func (pc *PeerConnection) IsTerminated() bool {
	switch pc.wrapped.ConnectionState() {
	case webrtc.PeerConnectionStateClosed:
	case webrtc.PeerConnectionStateDisconnected:
	case webrtc.PeerConnectionStateFailed:
		return true
	}

	return false
}

func (pc *PeerConnection) AddICECandidate(candidate webrtc.ICECandidateInit) error {
	return pc.wrapped.AddICECandidate(candidate)
}

func (pc *PeerConnection) CreateAnswer(options *webrtc.AnswerOptions) (webrtc.SessionDescription, error) {
	return pc.wrapped.CreateAnswer(options)
}

func (pc *PeerConnection) CreateOffer(options *webrtc.OfferOptions) (webrtc.SessionDescription, error) {
	return pc.wrapped.CreateOffer(options)
}

func (pc *PeerConnection) SetRemoteDescription(description webrtc.SessionDescription) error {
	return pc.wrapped.SetRemoteDescription(description)
}

func (pc *PeerConnection) SetLocalDescription(description webrtc.SessionDescription) error {
	return pc.wrapped.SetLocalDescription(description)
}

func (pc *PeerConnection) AddTrack(track webrtc.TrackLocal) (*webrtc.RTPSender, error) {
	return pc.wrapped.AddTrack(track)
}

func (pc *PeerConnection) RemoveTrack(sender *webrtc.RTPSender) error {
	return pc.wrapped.RemoveTrack(sender)
}
