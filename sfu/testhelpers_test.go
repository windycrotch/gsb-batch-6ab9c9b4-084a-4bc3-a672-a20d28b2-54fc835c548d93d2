package sfu

import (
	"fmt"
	"sync"
	"time"

	"atomirex.com/umbrella/razor"
)

// fakeRemoteClient is a RemoteClient used by tests. It records the lifecycle
// callbacks it was given so tests can drive establishment failure / teardown
// without any real network, and it can publish tracks on demand.
type fakeRemoteClient struct {
	BaseClient

	trunkurl string

	mu            sync.Mutex
	stopped       bool
	outgoing      map[string]*incomingTrack // umbrellaId -> advertised track
	statusCalls   int
	evalStateCall int

	// onFailed / onTerminated come from RemoteClientParameters via the factory.
	params *RemoteClientParameters
}

func (f *fakeRemoteClient) Label() string {
	if f.trunkurl != "" {
		return "fake relay " + f.trunkurl
	}
	return f.label
}

func (f *fakeRemoteClient) getStatus() *SFUStatusClient {
	f.mu.Lock()
	f.statusCalls++
	f.mu.Unlock()

	return &SFUStatusClient{Label: f.Label(), TrunkUrl: f.trunkurl}
}

func (f *fakeRemoteClient) stop() {
	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return
	}
	f.stopped = true
	f.mu.Unlock()
}

func (f *fakeRemoteClient) isStopped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

func (f *fakeRemoteClient) AddOutgoingTracksForIncomingTrack(t *incomingTrack) {
	f.mu.Lock()
	f.outgoing[t.UmbrellaID()] = t
	f.mu.Unlock()
}

func (f *fakeRemoteClient) RemoveOutgoingTracksForIncomingTrack(t *incomingTrack) {
	f.mu.Lock()
	delete(f.outgoing, t.UmbrellaID())
	f.mu.Unlock()
}

func (f *fakeRemoteClient) RequestEvalState() {
	f.mu.Lock()
	f.evalStateCall++
	f.mu.Unlock()
}

func (f *fakeRemoteClient) advertisedTracks() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.outgoing)
}

func (f *fakeRemoteClient) callOnFailed() {
	if f.params != nil && f.params.onFailed != nil {
		f.params.onFailed()
	}
}

func (f *fakeRemoteClient) callOnTerminated() {
	if f.params != nil && f.params.onTerminated != nil {
		f.params.onTerminated()
	}
}

// fakeRemoteClientFactory records every instance it creates per url, letting
// tests assert that at most one instance exists for an address.
type fakeRemoteClientFactory struct {
	mu        sync.Mutex
	instances map[string][]*fakeRemoteClient
}

func newFakeRemoteClientFactory() *fakeRemoteClientFactory {
	return &fakeRemoteClientFactory{instances: make(map[string][]*fakeRemoteClient)}
}

func (f *fakeRemoteClientFactory) NewClient(params *RemoteClientParameters) RemoteClient {
	f.mu.Lock()
	defer f.mu.Unlock()

	c := &fakeRemoteClient{
		BaseClient: BaseClient{
			label:           fmt.Sprintf("fake relay %s", params.trunkurl),
			logger:          params.logger,
			isRelay:         true,
			relayURL:        params.trunkurl,
			relayGeneration: params.relayGeneration,
		},
		trunkurl: params.trunkurl,
		outgoing: make(map[string]*incomingTrack),
		params:   params,
	}
	f.instances[params.trunkurl] = append(f.instances[params.trunkurl], c)
	return c
}

func (f *fakeRemoteClientFactory) countFor(url string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.instances[url])
}

func (f *fakeRemoteClientFactory) lastFor(url string) *fakeRemoteClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	list := f.instances[url]
	if len(list) == 0 {
		return nil
	}
	return list[len(list)-1]
}

// fakePeerConnectionFactory is unused by relay-only tests but must be non-nil
// for Sfu construction when dependencies are injected.
type fakePeerConnectionFactory struct{}

func (f *fakePeerConnectionFactory) NewPeerConnection(label string) (*PeerConnection, error) {
	return nil, fmt.Errorf("peer connections are not available in this test")
}

var _ RemoteClientFactory = (*fakeRemoteClientFactory)(nil)

// newTestSfu builds an Sfu with injected fake factories and no real networking.
func newTestSfu(t interface {
	Fatalf(format string, args ...any)
	Cleanup(func())
}) (*Sfu, *fakeRemoteClientFactory) {
	logger := razor.NewLogger(razor.LoggingLevelOff, false)

	clients := newFakeRemoteClientFactory()

	s := NewSfuWithDependencies(logger, 40000, 40100, nil, &SfuDependencies{
		PeerConnectionFactory: &fakePeerConnectionFactory{},
		RemoteClientFactory:   clients,
	})

	t.Cleanup(s.Close)

	return s, clients
}

// waitFor polls cond until it is true or the deadline elapses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}
