package sfu

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync/atomic"
	"time"

	"atomirex.com/umbrella/razor"
	"github.com/atomirex/mdns"
	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/logging"
	"github.com/pion/webrtc/v4"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// statusQueryTimeout bounds how long a status snapshot may wait for an
	// individual client to respond, so a wedged client cannot block the SFU.
	statusQueryTimeout = 2 * time.Second
)

type sfuCommand int

const (
	sfuAddOutgoingTracksForIncomingTrack sfuCommand = iota
	sfuRemoveAllOutgoingTracksForIncomingTrack
	sfuSignalClients
	sfuAddClient
	sfuRemoveClient

	sfuGetCurrentServers
	sfuSetCurrentServers

	sfuRelayFailed
	sfuRelayTerminated
	sfuShutdown

	sfuGetStatus
)

type sfuCommandMessage struct {
	intrack           *incomingTrack
	client            RemoteClient
	SetCurrentServers *CurrentServers

	relayURL        string
	relayGeneration uint64

	result *sfuCommandResult
}

type sfuCommandResult struct {
	servers chan *CurrentServers
	status  chan *SFUStatus
	done    chan struct{}
}

// relayLifecycle tracks the state of a relay (trunk) connection to another node.
// A relay is only ever represented by a single instance for a given url at any time.
type relayLifecycle int

const (
	relayDialing relayLifecycle = iota
	relayRunning
	relayStopping
)

// relayEntry is the SFU-owned record of one relay instance.
// The client pointer belongs to the entry so that stale callbacks from previous
// instances can never affect a newer instance for the same url.
type relayEntry struct {
	url        string
	generation uint64
	state      relayLifecycle
	client     RemoteClient
}

// trackRegistration tracks every source which is currently providing a track with
// the given umbrella id. Sources are reference counted: the track is only removed
// from the relaying set (and from clients) when the last source disappears.
type trackRegistration struct {
	umbrellaID string
	descriptor *TrackDescriptor
	sources    []*incomingTrack // sources[0] is the source currently being relayed
}

type Sfu struct {
	clients     []RemoteClient
	sfuCommands chan sfuCommandMessage

	peerConnectionFactory PeerConnectionFactory
	remoteClientFactory   RemoteClientFactory

	// UmbrellaID -> registration of all incoming tracks which are being relayed
	localTracks map[string]*trackRegistration

	handler *razor.MessageHandler[sfuCommand, sfuCommandMessage]

	intendedServers map[string]bool        // This works because strings completely define the spec of the server right now
	servers         map[string]*relayEntry // Relays which should be (or are being) established
	stoppingServers map[string]*relayEntry // Relays which have been asked to stop but have not fully torn down yet

	// Monotonic generation counter so late callbacks from an old relay instance
	// can be discarded instead of affecting a newer instance for the same url.
	nextRelayGeneration uint64

	shuttingDown atomic.Bool

	logger *razor.Logger

	loggerPion logging.LeveledLogger

	mdnsConn *mdns.Conn
}

func (s *Sfu) GetStatus() *SFUStatus {
	msg := sfuCommandMessage{result: &sfuCommandResult{
		status: make(chan *SFUStatus, 1),
	}}

	s.handler.Send(sfuGetStatus, &msg)

	return <-msg.result.status
}

func (s *Sfu) GetCurrentServers() *CurrentServers {
	msg := sfuCommandMessage{result: &sfuCommandResult{
		servers: make(chan *CurrentServers, 1),
	}}

	s.handler.Send(sfuGetCurrentServers, &msg)

	return <-msg.result.servers
}

func (s *Sfu) SetCurrentServers(update *CurrentServers) *CurrentServers {
	msg := sfuCommandMessage{
		SetCurrentServers: update,
		result: &sfuCommandResult{
			servers: make(chan *CurrentServers, 1),
		},
	}

	s.handler.Send(sfuSetCurrentServers, &msg)

	return <-msg.result.servers
}

// SfuDependencies allows the peer connection and remote client factories to be
// replaced, which is primarily useful for tests.
type SfuDependencies struct {
	PeerConnectionFactory PeerConnectionFactory
	RemoteClientFactory   RemoteClientFactory
}

func NewSfu(logger *razor.Logger, minPort uint16, maxPort uint16, ip *string) *Sfu {
	return NewSfuWithDependencies(logger, minPort, maxPort, ip, nil)
}

func NewSfuWithDependencies(logger *razor.Logger, minPort uint16, maxPort uint16, ip *string, deps *SfuDependencies) *Sfu {
	loggerPion := logging.NewDefaultLoggerFactory().NewLogger("sfu-ws")
	loggerPion.(*logging.DefaultLeveledLogger).SetLevel(logging.LogLevelError)

	s := &Sfu{
		sfuCommands:     make(chan sfuCommandMessage, 256),
		localTracks:     make(map[string]*trackRegistration),
		intendedServers: make(map[string]bool),
		servers:         make(map[string]*relayEntry),
		stoppingServers: make(map[string]*relayEntry),
		logger:          logger,
		loggerPion:      loggerPion,
	}

	if deps != nil {
		s.peerConnectionFactory = deps.PeerConnectionFactory
		s.remoteClientFactory = deps.RemoteClientFactory
	}

	if s.peerConnectionFactory == nil {
		settingEngine := webrtc.SettingEngine{}

		settingEngine.SetEphemeralUDPPortRange(minPort, maxPort)

		if ip != nil {
			settingEngine.SetNAT1To1IPs([]string{*ip}, webrtc.ICECandidateTypeHost)
		}

		m := &webrtc.MediaEngine{}
		if err := m.RegisterDefaultCodecs(); err != nil {
			panic("Error setting default codecs")
		}

		// Now we know what this is and why . . . . facepalm
		// This is the "default" nack, sr, rr etc. handling for rtcp
		// We can come back to it when it's a problem
		interceptorRegistry := &interceptor.Registry{}
		if err := webrtc.RegisterDefaultInterceptors(m, interceptorRegistry); err != nil {
			panic("Panic setting interceptors")
		}

		webrtcApi := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine), webrtc.WithMediaEngine(m))

		s.peerConnectionFactory = &PionPeerConnectionFactory{
			pcConfig: &webrtc.Configuration{
				ICEServers: []webrtc.ICEServer{
					{
						URLs: []string{"stun:stun.l.google.com:19302"},
					},
				},
			},
			webrtcApi: webrtcApi,
			logger:    logger,
		}
	}

	if s.remoteClientFactory == nil {
		s.remoteClientFactory = &DefaultRemoteClientFactory{}
	}

	s.handler = razor.NewMessageHandler(logger, "sfu", 1024, func(what sfuCommand, payload *sfuCommandMessage) bool {
		shouldSignalClients := false

		switch what {
		case sfuAddClient:
			if !s.isActiveRelay(payload.client) {
				// A relay which has already been removed must never be able to
				// register itself as a client (avoids "half connected" zombies).
				payload.client.stop()
				return true
			}

			s.clients = append(s.clients, payload.client)

			if url, generation, ok := payload.client.RelayIdentity(); ok {
				if entry, exists := s.servers[url]; exists && entry.generation == generation {
					entry.state = relayRunning
				}
			}

			// Add all existing tracks
			for _, t := range s.localTracks {
				if len(t.sources) > 0 {
					payload.client.AddOutgoingTracksForIncomingTrack(t.sources[0])
				}
			}

			shouldSignalClients = true
		case sfuRemoveClient:
			if s.removeClientFromList(payload.client) {
				shouldSignalClients = true
			}
		case sfuAddOutgoingTracksForIncomingTrack:
			intrack := payload.intrack

			if !s.isTrackOwnerActive(intrack) {
				// A track introduced by a relay which has already been removed
				// must not be re-advertised to clients.
				logger.Info("sfu", "ignoring track from inactive owner: "+intrack.String())
				return true
			}

			logger.Info("sfu", "adding track: "+intrack.String())
			isNew, replacedActive, previous := s.registerTrackSource(intrack)

			if isNew {
				for _, c := range s.clients {
					c.AddOutgoingTracksForIncomingTrack(intrack)
				}
			} else if replacedActive {
				// Same owner re-advertised the track with a new source, so clients
				// have to swap the underlying relay track.
				for _, c := range s.clients {
					c.RemoveOutgoingTracksForIncomingTrack(previous)
					c.AddOutgoingTracksForIncomingTrack(intrack)
				}
			}

			shouldSignalClients = true
		case sfuRemoveAllOutgoingTracksForIncomingTrack:
			logger.Info("sfu", "removing outgoing track source: "+payload.intrack.String())

			removed, wasActive, remaining, next := s.unregisterTrackSource(payload.intrack)
			if !removed {
				return true
			}

			if !remaining {
				for _, c := range s.clients {
					c.RemoveOutgoingTracksForIncomingTrack(payload.intrack)
				}
			} else if wasActive {
				for _, c := range s.clients {
					c.RemoveOutgoingTracksForIncomingTrack(payload.intrack)
					c.AddOutgoingTracksForIncomingTrack(next)
				}
			}

			shouldSignalClients = true
		case sfuSignalClients:
			shouldSignalClients = true
		case sfuGetStatus:
			// Terrible version of this which blocks while polling everything
			// Should really pass around a thing that gathers the info
			// but in reality I should move to a pub/sub thing that pushes the data out on changes anyway
			// this is to debug the mess as is :P
			logger.Info("sfu", "SFU getting status servers")
			servers := s.currentServerList()
			logger.Info("sfu", "SFU getting status relaying")
			relaying := s.currentRelayingTracks()
			logger.Info("sfu", "SFU getting status clients")
			clients := make([]*SFUStatusClient, 0)
			for _, c := range s.clients {
				logger.Info("sfu", "SFU getting status for client "+c.Label())
				st := c.getStatus()
				if st != nil {
					clients = append(clients, st)
				}
				logger.Info("sfu", "SFU received status for client "+c.Label())
			}

			logger.Info("sfu", "SFU getting status returning")
			status := &SFUStatus{
				RelayingTracks: relaying,
				Servers:        servers,
				Clients:        clients,
			}

			payload.result.status <- status
		case sfuGetCurrentServers:
			payload.result.servers <- &CurrentServers{Servers: s.currentServerList()}
		case sfuSetCurrentServers:
			// "intended" servers model, then regularly evaluate, so setup/teardown repeatedly is ok
			mentioned := make(map[string]bool)
			for _, t := range payload.SetCurrentServers.Servers {
				if t == "" {
					continue
				}
				mentioned[t] = true
			}

			// Just overwrite it with what the client sent! (For now)
			s.intendedServers = mentioned

			s.evaluateServers()

			shouldSignalClients = true

			payload.result.servers <- &CurrentServers{Servers: s.currentServerList()}
		case sfuRelayFailed:
			// The relay could not be established. Drop it entirely (including the
			// intent) so that resetting the same address later retries from clean.
			if entry, ok := s.servers[payload.relayURL]; ok && entry.generation == payload.relayGeneration {
				delete(s.servers, payload.relayURL)
				delete(s.intendedServers, payload.relayURL)
				s.removeTracksOwnedByEntry(entry)
				s.removeClientFromList(entry.client)
				if entry.client != nil {
					entry.client.stop()
				}
				shouldSignalClients = true
				s.evaluateServers()
			} else if entry, ok := s.stoppingServers[payload.relayURL]; ok && entry.generation == payload.relayGeneration {
				delete(s.stoppingServers, payload.relayURL)
				s.removeTracksOwnedByEntry(entry)
				s.removeClientFromList(entry.client)
				if entry.client != nil {
					entry.client.stop()
				}
				shouldSignalClients = true
				s.evaluateServers()
			}
		case sfuRelayTerminated:
			// The relay has fully torn down (either because we asked it to stop,
			// or because the connection was lost).
			if entry, ok := s.stoppingServers[payload.relayURL]; ok && entry.generation == payload.relayGeneration {
				delete(s.stoppingServers, payload.relayURL)
				s.removeTracksOwnedByEntry(entry)
				s.removeClientFromList(entry.client)
				shouldSignalClients = true
				s.evaluateServers()
			} else if entry, ok := s.servers[payload.relayURL]; ok && entry.generation == payload.relayGeneration {
				// Unexpected termination: forget it entirely, a later submission
				// of the same address will establish a fresh relay.
				delete(s.servers, payload.relayURL)
				delete(s.intendedServers, payload.relayURL)
				s.removeTracksOwnedByEntry(entry)
				s.removeClientFromList(entry.client)
				shouldSignalClients = true
				s.evaluateServers()
			}
		case sfuShutdown:
			for _, entry := range s.servers {
				s.removeTracksOwnedByEntry(entry)
				s.removeClientFromList(entry.client)
				if entry.client != nil {
					entry.client.stop()
				}
			}
			for _, entry := range s.stoppingServers {
				if entry.client != nil {
					entry.client.stop()
				}
			}
			s.servers = make(map[string]*relayEntry)
			s.stoppingServers = make(map[string]*relayEntry)
			s.intendedServers = make(map[string]bool)
			s.localTracks = make(map[string]*trackRegistration)
			s.clients = nil

			if payload.result != nil && payload.result.done != nil {
				close(payload.result.done)
			}
		}

		if shouldSignalClients {
			logger.Verbose("sfu", "SFU should signaling clients")
			s.handler.Cancel(sfuSignalClients)

			for _, c := range s.clients {
				logger.Verbose("sfu", "SFU signaling client "+c.Label())
				c.RequestEvalState()
			}

			logger.Verbose("sfu", "SFU finished signaling clients")
		}

		return true
	})

	s.handler.Loop(func() {
		if !s.shuttingDown.Load() {
			panic("SFU unexpectedly terminated")
		}
	})

	return s
}

// Close stops all relays and terminates the SFU command loop. It is intended for
// orderly shutdown (and for tests); the SFU is not usable afterwards.
func (s *Sfu) Close() {
	msg := sfuCommandMessage{result: &sfuCommandResult{done: make(chan struct{}, 1)}}

	if s.handler.Send(sfuShutdown, &msg) {
		<-msg.result.done
	}

	s.shuttingDown.Store(true)
	s.handler.Abort()
}

// currentServerList returns the converged desired set of relays. A relay which
// is still establishing is included (it was submitted), while a relay which was
// removed, failed, or lost its connection is not (its intent is cleared).
func (s *Sfu) currentServerList() []string {
	servers := make([]string, 0, len(s.intendedServers))
	for url := range s.intendedServers {
		servers = append(servers, url)
	}
	sort.Strings(servers)
	return servers
}

func (s *Sfu) currentRelayingTracks() []*TrackDescriptor {
	ids := make([]string, 0, len(s.localTracks))
	for id := range s.localTracks {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	relaying := make([]*TrackDescriptor, 0, len(ids))
	for _, id := range ids {
		reg := s.localTracks[id]
		if reg == nil || len(reg.sources) == 0 {
			continue
		}
		relaying = append(relaying, reg.descriptor)
	}

	return relaying
}

func (s *Sfu) removeClientFromList(c RemoteClient) bool {
	if c == nil {
		return false
	}

	for i, existing := range s.clients {
		if existing == c {
			s.clients = append(s.clients[:i], s.clients[i+1:]...)
			return true
		}
	}

	return false
}

func (s *Sfu) isActiveRelay(c RemoteClient) bool {
	if c == nil || !c.IsRelay() {
		return true
	}

	url, generation, ok := c.RelayIdentity()
	if !ok {
		return false
	}

	// The instance may be registering itself before NewClient has returned and
	// the entry has been stored, so identity (url + generation) is authoritative
	// rather than the client pointer.
	entry, exists := s.servers[url]
	if !exists || entry.generation != generation {
		return false
	}

	return true
}

func (s *Sfu) isTrackOwnerActive(t *incomingTrack) bool {
	if t.owner == nil || !t.owner.IsRelay() {
		return true
	}

	return s.isActiveRelay(t.owner)
}

func (s *Sfu) registerTrackSource(intrack *incomingTrack) (isNew bool, replacedActive bool, previous *incomingTrack) {
	id := intrack.UmbrellaID()

	reg, exists := s.localTracks[id]
	if !exists {
		s.localTracks[id] = &trackRegistration{
			umbrellaID: id,
			descriptor: intrack.descriptor,
			sources:    []*incomingTrack{intrack},
		}
		return true, false, nil
	}

	for i, src := range reg.sources {
		if src == intrack {
			return false, false, nil
		}

		// The same owner re-advertising a track for the same umbrella id replaces
		// its previous source rather than accumulating stale references.
		if intrack.owner != nil && src.owner == intrack.owner {
			reg.sources[i] = intrack
			if i == 0 {
				reg.descriptor = intrack.descriptor
			}
			return false, i == 0, src
		}
	}

	reg.sources = append(reg.sources, intrack)
	return false, false, nil
}

func (s *Sfu) unregisterTrackSource(intrack *incomingTrack) (removed bool, wasActive bool, remaining bool, next *incomingTrack) {
	if intrack == nil {
		return false, false, false, nil
	}

	id := intrack.UmbrellaID()
	reg, exists := s.localTracks[id]
	if !exists {
		return false, false, false, nil
	}

	index := -1
	for i, src := range reg.sources {
		if src == intrack {
			index = i
			break
		}
	}

	if index < 0 {
		return false, false, false, nil
	}

	reg.sources = append(reg.sources[:index], reg.sources[index+1:]...)

	if len(reg.sources) == 0 {
		delete(s.localTracks, id)
		return true, index == 0, false, nil
	}

	if index == 0 {
		reg.descriptor = reg.sources[0].descriptor
		return true, true, true, reg.sources[0]
	}

	return true, false, true, nil
}

// removeTracksOwnedByEntry removes every track source introduced by the relay
// instance described by entry. Matching is done by the instance identity
// (url + generation) rather than the client pointer, so it also works for a
// relay which failed before the SFU stored its client pointer. It is called
// when a relay goes away so that its tracks disappear from the relaying set in
// the same (serialized) step as the relay itself disappears from the server
// list. Must only be called from the SFU command loop.
func (s *Sfu) removeTracksOwnedByEntry(entry *relayEntry) {
	if entry == nil {
		return
	}

	owned := func(owner RemoteClient) bool {
		if owner == nil {
			return false
		}
		if entry.client != nil && owner == entry.client {
			return true
		}
		url, generation, ok := owner.RelayIdentity()
		return ok && url == entry.url && generation == entry.generation
	}

	for id, reg := range s.localTracks {
		for {
			index := -1
			for i, src := range reg.sources {
				if owned(src.owner) {
					index = i
					break
				}
			}

			if index < 0 {
				break
			}

			removed := reg.sources[index]
			reg.sources = append(reg.sources[:index], reg.sources[index+1:]...)

			if len(reg.sources) == 0 {
				delete(s.localTracks, id)
				for _, c := range s.clients {
					c.RemoveOutgoingTracksForIncomingTrack(removed)
				}
			} else if index == 0 {
				reg.descriptor = reg.sources[0].descriptor
				for _, c := range s.clients {
					c.RemoveOutgoingTracksForIncomingTrack(removed)
					c.AddOutgoingTracksForIncomingTrack(reg.sources[0])
				}
			}
		}
	}
}

func (s *Sfu) evaluateServers() {
	// Ensure any running servers that should be stopped are stopping or stopped.
	// They are removed from the published state immediately so that /servers and
	// /status never report a relay which is no longer desired.
	for url, entry := range s.servers {
		if s.intendedServers[url] {
			continue
		}

		entry.state = relayStopping
		delete(s.servers, url)
		s.stoppingServers[url] = entry

		s.removeTracksOwnedByEntry(entry)
		s.removeClientFromList(entry.client)

		if entry.client != nil {
			entry.client.stop()
		}
	}

	// Create and start any servers that should exist but do not.
	// A url which is still tearing down is not re-created until its previous
	// instance has fully terminated, so never more than one instance exists.
	for url := range s.intendedServers {
		if _, exists := s.servers[url]; exists {
			continue
		}

		if _, stopping := s.stoppingServers[url]; stopping {
			continue
		}

		s.startRelay(url)
	}
}

func (s *Sfu) startRelay(url string) {
	s.nextRelayGeneration++
	generation := s.nextRelayGeneration

	entry := &relayEntry{
		url:        url,
		generation: generation,
		state:      relayDialing,
	}
	s.servers[url] = entry

	client := s.remoteClientFactory.NewClient(&RemoteClientParameters{
		logger:          s.logger,
		trunkurl:        url,
		s:               s,
		relayGeneration: generation,
		onFailed: func() {
			s.handler.Send(sfuRelayFailed, &sfuCommandMessage{
				relayURL:        url,
				relayGeneration: generation,
			})
		},
		onTerminated: func() {
			s.handler.Send(sfuRelayTerminated, &sfuCommandMessage{
				relayURL:        url,
				relayGeneration: generation,
			})
		},
	})

	entry.client = client

	// A fake (or otherwise synchronous) factory may already have reported a
	// failure from inside NewClient, in which case this entry is stale and must
	// not be published. The failure command is queued on the handler, so the
	// check below is just defensive for factories which call back synchronously.
	if existing, ok := s.servers[url]; !ok || existing != entry {
		return
	}
}

func (s *Sfu) addTrack(intrack *incomingTrack) {
	s.handler.Send(sfuAddOutgoingTracksForIncomingTrack, &sfuCommandMessage{intrack: intrack})
}

func (s *Sfu) removeOutgoingTracksForIncomingTrack(t *incomingTrack) {
	s.handler.Send(sfuRemoveAllOutgoingTracksForIncomingTrack, &sfuCommandMessage{intrack: t})
}

func (s *Sfu) WebsocketHandler(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("sfu", "Failed to upgrade HTTP to Websocket: "+err.Error())
		return
	}

	s.remoteClientFactory.NewClient(&RemoteClientParameters{logger: s.logger, ws: ws, s: s})
}

func (s *Sfu) SetMdnsConn(mdnsConn *mdns.Conn) {
	s.mdnsConn = mdnsConn
}

// Use our own mdns for resolving .local addresses to work around compatibility problems
// If we are in cloud mode this will return an error
// If the host is not found this will return an error
// If the host is found the string returned will contain the IP as a string . . .
func (s *Sfu) mdnsLookup(hostname string) (string, error) {
	if s.mdnsConn == nil {
		return "", fmt.Errorf("attempting to resolve local subnet host while mdns not in use")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, addr, err := s.mdnsConn.QueryAddr(ctx, hostname)
	if err != nil {
		return "", err
	} else {
		return addr.String(), nil
	}
}
