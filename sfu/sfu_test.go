package sfu

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

const testServerURL = "wss://relay.example.test/wsb"

func newInputTrack(umbrellaID string, owner RemoteClient) *incomingTrack {
	return &incomingTrack{
		descriptor: &TrackDescriptor{
			UmbrellaId: umbrellaID,
			Kind:       TrackKind_Audio,
		},
		owner: owner,
	}
}

func addViewer(s *Sfu) *fakeRemoteClient {
	viewer := &fakeRemoteClient{
		BaseClient: BaseClient{label: "viewer", logger: s.logger},
		outgoing:   make(map[string]*incomingTrack),
	}
	s.handler.Send(sfuAddClient, &sfuCommandMessage{client: viewer})
	return viewer
}

func submitServers(s *Sfu, servers ...string) *CurrentServers {
	return s.SetCurrentServers(&CurrentServers{Servers: servers})
}

func TestRepeatedSameServerSubmissionYieldsSingleRelay(t *testing.T) {
	s, factory := newTestSfu(t)

	for i := 0; i < 5; i++ {
		result := submitServers(s, testServerURL)
		if len(result.Servers) != 1 || result.Servers[0] != testServerURL {
			t.Fatalf("submission %d: expected exactly one server, got %v", i, result.Servers)
		}
	}

	// Concurrent duplicate submissions must not fan out additional instances.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			submitServers(s, testServerURL)
		}()
	}
	wg.Wait()

	servers := s.GetCurrentServers()
	if len(servers.Servers) != 1 || servers.Servers[0] != testServerURL {
		t.Fatalf("expected a single server in GET, got %v", servers.Servers)
	}

	if got := factory.countFor(testServerURL); got != 1 {
		t.Fatalf("expected exactly one relay instance to be created, got %d", got)
	}

	status := s.GetStatus()
	if len(status.Servers) != 1 || status.Servers[0] != testServerURL {
		t.Fatalf("expected a single server in status, got %v", status.Servers)
	}
}

func TestRemoveServerRemovesFromServersAndStatus(t *testing.T) {
	s, factory := newTestSfu(t)

	submitServers(s, testServerURL)
	relay := factory.lastFor(testServerURL)
	relay.callOnTerminated() // simulate the fake relay reaching running state cleanly

	// Re-add so the relay is (re)created and considered active.
	submitServers(s, testServerURL)
	relay = factory.lastFor(testServerURL)

	viewer := addViewer(s)
	track := newInputTrack("umb-1", relay)
	s.addTrack(track)

	if !waitFor(2*time.Second, func() bool {
		return len(s.GetStatus().RelayingTracks) == 1
	}) {
		t.Fatalf("expected the track to be relaying")
	}

	if !waitFor(2*time.Second, func() bool { return viewer.advertisedTracks() == 1 }) {
		t.Fatalf("expected the track to be advertised to the viewer")
	}

	submitServers(s) // remove everything

	if !waitFor(2*time.Second, func() bool { return len(s.GetCurrentServers().Servers) == 0 }) {
		t.Fatalf("expected the server list to become empty")
	}

	status := s.GetStatus()
	if len(status.Servers) != 0 {
		t.Fatalf("expected no servers in status, got %v", status.Servers)
	}

	// A snapshot must never list relaying tracks whose only owner has stopped.
	if len(status.RelayingTracks) != 0 {
		t.Fatalf("expected no relaying tracks after removal, got %v", status.RelayingTracks)
	}

	if !relay.isStopped() {
		t.Fatalf("expected the relay to have been stopped")
	}

	if !waitFor(2*time.Second, func() bool { return viewer.advertisedTracks() == 0 }) {
		t.Fatalf("expected the track to be withdrawn from the viewer")
	}
}

func TestRemoveDuringEstablishmentLeavesNoHalfConnection(t *testing.T) {
	s, factory := newTestSfu(t)

	submitServers(s, testServerURL)
	dialing := factory.lastFor(testServerURL)
	if dialing == nil {
		t.Fatalf("expected a relay instance to be dialling")
	}

	// Removing while the relay is still establishing must stop it.
	submitServers(s)
	if !dialing.isStopped() {
		t.Fatalf("expected the dialling relay to be stopped when removed")
	}

	// Re-adding the same address while the old instance is still tearing down
	// must not create a second, overlapping instance.
	submitServers(s, testServerURL)
	if got := factory.countFor(testServerURL); got != 1 {
		t.Fatalf("expected no replacement while the previous instance is tearing down, got %d instances", got)
	}

	// Once the old instance reports full termination, a fresh one is created.
	dialing.callOnTerminated()

	if !waitFor(2*time.Second, func() bool { return factory.countFor(testServerURL) == 2 }) {
		t.Fatalf("expected a replacement instance after the previous one terminated, got %d", factory.countFor(testServerURL))
	}

	if got := len(s.GetCurrentServers().Servers); got != 1 {
		t.Fatalf("expected exactly one server after replacement, got %d", got)
	}
}

func TestFailedRelayCanBeRetried(t *testing.T) {
	s, factory := newTestSfu(t)

	submitServers(s, testServerURL)
	failed := factory.lastFor(testServerURL)

	failed.callOnFailed()

	if !waitFor(2*time.Second, func() bool { return len(s.GetCurrentServers().Servers) == 0 }) {
		t.Fatalf("a failed relay must not remain in the server list")
	}

	status := s.GetStatus()
	if len(status.Servers) != 0 {
		t.Fatalf("a failed relay must not remain in the status server list, got %v", status.Servers)
	}

	// After the failure the instance must also have been asked to stop.
	if !failed.isStopped() {
		t.Fatalf("expected the failed relay instance to be stopped")
	}

	// Re-submitting the same address must attempt a fresh connection.
	result := submitServers(s, testServerURL)
	if len(result.Servers) != 1 {
		t.Fatalf("expected the server to be present after re-submission, got %v", result.Servers)
	}

	if !waitFor(2*time.Second, func() bool { return factory.countFor(testServerURL) == 2 }) {
		t.Fatalf("expected a new connection attempt after re-submitting a failed address, got %d", factory.countFor(testServerURL))
	}
}

func TestTrackRemovedWhenRelayTerminates(t *testing.T) {
	s, factory := newTestSfu(t)

	submitServers(s, testServerURL)
	relay := factory.lastFor(testServerURL)

	viewer := addViewer(s)
	s.addTrack(newInputTrack("umb-1", relay))

	if !waitFor(2*time.Second, func() bool { return len(s.GetStatus().RelayingTracks) == 1 }) {
		t.Fatalf("expected the track to be relaying")
	}

	// Simulate the relay connection dropping.
	relay.callOnTerminated()

	if !waitFor(2*time.Second, func() bool {
		status := s.GetStatus()
		return len(status.Servers) == 0 && len(status.RelayingTracks) == 0
	}) {
		status := s.GetStatus()
		t.Fatalf("expected both the server and its track to disappear, got servers=%v tracks=%v",
			status.Servers, status.RelayingTracks)
	}

	if !waitFor(2*time.Second, func() bool { return viewer.advertisedTracks() == 0 }) {
		t.Fatalf("expected the track to be withdrawn from the viewer")
	}
}

func TestSharedTrackStopsOnlyWhenLastOwnerRemoved(t *testing.T) {
	s, factory := newTestSfu(t)

	submitServers(s, "wss://relay-one.example.test/wsb", "wss://relay-two.example.test/wsb")
	relayOne := factory.lastFor("wss://relay-one.example.test/wsb")
	relayTwo := factory.lastFor("wss://relay-two.example.test/wsb")

	trackOne := newInputTrack("shared-umb", relayOne)
	trackTwo := newInputTrack("shared-umb", relayTwo)

	s.addTrack(trackOne)
	s.addTrack(trackTwo)

	if !waitFor(2*time.Second, func() bool {
		return len(s.GetStatus().RelayingTracks) == 1
	}) {
		t.Fatalf("expected exactly one relaying track (deduplicated)")
	}

	// Removing one owner must keep the track relaying.
	s.removeOutgoingTracksForIncomingTrack(trackOne)

	time.Sleep(150 * time.Millisecond)
	status := s.GetStatus()
	if len(status.RelayingTracks) != 1 || status.RelayingTracks[0].UmbrellaId != "shared-umb" {
		t.Fatalf("expected the shared track to keep relaying while another owner remains, got %v", status.RelayingTracks)
	}

	// Removing the last owner stops it.
	s.removeOutgoingTracksForIncomingTrack(trackTwo)

	if !waitFor(2*time.Second, func() bool { return len(s.GetStatus().RelayingTracks) == 0 }) {
		t.Fatalf("expected the shared track to stop once the last owner disappeared")
	}
}

func TestConcurrentSubmitAndRemoveConverges(t *testing.T) {
	s, _ := newTestSfu(t)

	const otherURL = "wss://relay-other.example.test/wsb"

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				submitServers(s, testServerURL)
			case 1:
				submitServers(s, otherURL)
			default:
				submitServers(s)
			}
		}(i)
	}
	wg.Wait()

	// The final intent is unambiguous and must win.
	final := submitServers(s, testServerURL)

	if !waitFor(5*time.Second, func() bool {
		return s.GetCurrentServers().Servers[0] == testServerURL && len(s.GetCurrentServers().Servers) == 1
	}) {
		t.Fatalf("expected convergence to the final submission, got %v", s.GetCurrentServers().Servers)
	}

	if len(final.Servers) != 1 || final.Servers[0] != testServerURL {
		t.Fatalf("expected the final submission result to contain only the final server, got %v", final.Servers)
	}

	status := s.GetStatus()
	if len(status.Servers) != 1 || status.Servers[0] != testServerURL {
		t.Fatalf("expected status to converge to the final submission, got %v", status.Servers)
	}
}

func TestHTTPEntryPointsConverge(t *testing.T) {
	s, factory := newTestSfu(t)

	handler := http.NewServeMux()
	handler.HandleFunc("/servers", func(w http.ResponseWriter, r *http.Request) {
		if !s.HandleServers(w, r) {
			http.NotFound(w, r)
		}
	})
	handler.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if !s.HandleStatus(w, r) {
			http.NotFound(w, r)
		}
	})

	postServers := func(servers ...string) *CurrentServers {
		body, err := proto.Marshal(&CurrentServers{Servers: servers})
		if err != nil {
			t.Fatalf("failed to marshal request: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/servers", bytes.NewReader(body))
		req.Header.Set("Content-Type", protobufContentType)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
		}

		var result CurrentServers
		if err := proto.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		return &result
	}

	getServers := func() *CurrentServers {
		req := httptest.NewRequest(http.MethodGet, "/servers", nil)
		req.Header.Set("Content-Type", protobufContentType)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
		}

		var result CurrentServers
		if err := proto.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		return &result
	}

	getStatus := func() *SFUStatus {
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		req.Header.Set("Content-Type", protobufContentType)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
		}

		var result SFUStatus
		if err := proto.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		return &result
	}

	// Repeated submission of the same address yields one entry.
	for i := 0; i < 3; i++ {
		postServers(testServerURL)
	}

	if got := getServers().Servers; len(got) != 1 || got[0] != testServerURL {
		t.Fatalf("expected one server from GET, got %v", got)
	}
	if got := getStatus().Servers; len(got) != 1 || got[0] != testServerURL {
		t.Fatalf("expected one server from status, got %v", got)
	}
	if got := factory.countFor(testServerURL); got != 1 {
		t.Fatalf("expected one relay instance, got %d", got)
	}

	// Removal hides the address from both endpoints.
	postServers()

	if got := getServers().Servers; len(got) != 0 {
		t.Fatalf("expected no servers from GET after removal, got %v", got)
	}
	status := getStatus()
	if len(status.Servers) != 0 {
		t.Fatalf("expected no servers from status after removal, got %v", status.Servers)
	}
	if len(status.RelayingTracks) != 0 {
		t.Fatalf("expected no relaying tracks after removal, got %v", status.RelayingTracks)
	}
}

func TestFailedRelayTracksAreCleanedUpEvenBeforeClientIsStored(t *testing.T) {
	s, factory := newTestSfu(t)

	viewer := addViewer(s)

	// Simulate a relay which advertises a track and then fails before the SFU
	// stores its client pointer (the real client can dial, register and fail
	// from inside NewClient).
	submitServers(s, testServerURL)
	relay := factory.lastFor(testServerURL)

	track := newInputTrack("umb-early-fail", relay)
	s.addTrack(track)

	if !waitFor(2*time.Second, func() bool { return len(s.GetStatus().RelayingTracks) == 1 }) {
		t.Fatalf("expected the track to be relaying before the failure")
	}

	relay.callOnFailed()

	if !waitFor(2*time.Second, func() bool {
		status := s.GetStatus()
		return len(status.Servers) == 0 && len(status.RelayingTracks) == 0
	}) {
		status := s.GetStatus()
		t.Fatalf("expected the failed relay's track to be cleaned up, got servers=%v tracks=%v",
			status.Servers, status.RelayingTracks)
	}

	if !waitFor(2*time.Second, func() bool { return viewer.advertisedTracks() == 0 }) {
		t.Fatalf("expected the failed relay's track to be withdrawn from the viewer")
	}

	// Its late clean-up notification must not disturb the converged state.
	relay.callOnTerminated()
	time.Sleep(100 * time.Millisecond)

	status := s.GetStatus()
	if len(status.Servers) != 0 || len(status.RelayingTracks) != 0 {
		t.Fatalf("stale termination of a failed relay changed the state: servers=%v tracks=%v",
			status.Servers, status.RelayingTracks)
	}
}

func TestStaleTerminationDoesNotAffectReplacement(t *testing.T) {
	s, factory := newTestSfu(t)

	submitServers(s, testServerURL)
	old := factory.lastFor(testServerURL)

	// Remove and immediately re-submit: the old instance is still tearing down.
	submitServers(s)
	submitServers(s, testServerURL)
	old.callOnTerminated()

	if !waitFor(2*time.Second, func() bool { return factory.countFor(testServerURL) == 2 }) {
		t.Fatalf("expected a replacement instance after the old one terminated")
	}

	replacement := factory.lastFor(testServerURL)

	// A duplicate late notification from the old instance must not remove the
	// replacement from the server list.
	old.callOnTerminated()
	time.Sleep(100 * time.Millisecond)

	status := s.GetStatus()
	if len(status.Servers) != 1 || status.Servers[0] != testServerURL {
		t.Fatalf("expected the replacement to remain, got %v", status.Servers)
	}

	if !s.isActiveRelay(replacement) {
		t.Fatalf("expected the replacement relay to remain active")
	}
}
