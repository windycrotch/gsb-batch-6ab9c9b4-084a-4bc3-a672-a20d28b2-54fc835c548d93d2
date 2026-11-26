package sfu

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"atomirex.com/umbrella/razor"
)

// newRealTestSfu builds an Sfu with the real relay client implementation (and
// real Pion peer connections), but only ever dials local addresses supplied by
// the test, so it never touches an external network.
func newRealTestSfu(t *testing.T) *Sfu {
	t.Helper()

	logger := razor.NewLogger(razor.LoggingLevelOff, false)
	s := NewSfu(logger, 41000, 41100, nil)
	t.Cleanup(s.Close)

	return s
}

func wsURLFor(addr string) string {
	return "ws://" + addr + "/wsb"
}

// testRelayServer is a minimal trunk peer: it upgrades to a websocket and then
// keeps the connection open until it is closed.
type testRelayServer struct {
	server   *httptest.Server
	upgrades atomic.Int32
	open     atomic.Int32
}

func startTestRelayServer(t *testing.T) *testRelayServer {
	t.Helper()

	trs := &testRelayServer{}

	trs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		trs.upgrades.Add(1)
		trs.open.Add(1)
		defer trs.open.Add(-1)
		defer conn.Close()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(trs.server.Close)

	return trs
}

func (trs *testRelayServer) url() string {
	return wsURLFor(trs.server.Listener.Addr().String())
}

func waitForStatus(t *testing.T, s *Sfu, timeout time.Duration, cond func(*SFUStatus) bool) *SFUStatus {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last *SFUStatus
	for time.Now().Before(deadline) {
		last = s.GetStatus()
		if cond(last) {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	return last
}

func hasServer(status *SFUStatus, url string) bool {
	for _, s := range status.Servers {
		if s == url {
			return true
		}
	}
	return false
}

func TestRealRelayConnectsAndStopsCleanly(t *testing.T) {
	s := newRealTestSfu(t)
	server := startTestRelayServer(t)
	url := server.url()

	submitServers(s, url)

	status := waitForStatus(t, s, 5*time.Second, func(st *SFUStatus) bool {
		return hasServer(st, url) && len(st.Clients) == 1
	})

	if !hasServer(status, url) {
		t.Fatalf("expected the relay server to be connected, got %v", status.Servers)
	}
	if len(status.Clients) != 1 || status.Clients[0].TrunkUrl != url {
		t.Fatalf("expected exactly one relay client for %s, got %+v", url, status.Clients)
	}

	if !waitFor(5*time.Second, func() bool { return server.open.Load() == 1 }) {
		t.Fatalf("expected the relay to establish a websocket connection")
	}

	submitServers(s)

	status = waitForStatus(t, s, 5*time.Second, func(st *SFUStatus) bool {
		return len(st.Servers) == 0 && len(st.Clients) == 0 && len(st.RelayingTracks) == 0
	})

	if len(status.Servers) != 0 || len(status.Clients) != 0 || len(status.RelayingTracks) != 0 {
		t.Fatalf("expected a fully converged empty state, got %+v", status)
	}

	if got := s.GetCurrentServers().Servers; len(got) != 0 {
		t.Fatalf("expected /servers to be empty after removal, got %v", got)
	}

	if !waitFor(5*time.Second, func() bool { return server.open.Load() == 0 }) {
		t.Fatalf("expected the relay websocket to be closed after removal")
	}

	// Re-submitting after a clean stop must establish exactly one new relay.
	submitServers(s, url)
	waitForStatus(t, s, 5*time.Second, func(st *SFUStatus) bool {
		return hasServer(st, url) && len(st.Clients) == 1
	})

	if got := server.upgrades.Load(); got != 2 {
		t.Fatalf("expected exactly two connections over the whole test, got %d", got)
	}
}

func TestRealRelayFailureAllowsRetry(t *testing.T) {
	s := newRealTestSfu(t)

	// Reserve an address and close it so the first dial is refused.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a local address: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	url := wsURLFor(addr)

	submitServers(s, url)

	// The failed relay must not remain in either server list.
	status := waitForStatus(t, s, 10*time.Second, func(st *SFUStatus) bool {
		return len(st.Servers) == 0 && len(st.Clients) == 0
	})
	if len(status.Servers) != 0 {
		t.Fatalf("expected the failed relay to be dropped, got %v", status.Servers)
	}
	if got := s.GetCurrentServers().Servers; len(got) != 0 {
		t.Fatalf("expected /servers to be empty after a failure, got %v", got)
	}

	// Now make the same address reachable and re-submit: the connection must be
	// attempted again rather than the address being stuck as failed.
	server := startTestRelayServerOnAddr(t, addr)

	submitServers(s, url)

	status = waitForStatus(t, s, 10*time.Second, func(st *SFUStatus) bool {
		return hasServer(st, url) && len(st.Clients) == 1
	})
	if !hasServer(status, url) || len(status.Clients) != 1 {
		t.Fatalf("expected the retry to connect, got servers=%v clients=%d", status.Servers, len(status.Clients))
	}
	if !waitFor(5*time.Second, func() bool { return server.open.Load() == 1 }) {
		t.Fatalf("expected a fresh websocket connection after retry")
	}
}

// startTestRelayServerOnAddr serves the same minimal trunk peer on a specific
// address (used to prove that a previously failing address can be retried).
func startTestRelayServerOnAddr(t *testing.T, addr string) *testRelayServer {
	t.Helper()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", addr, err)
	}

	trs := &testRelayServer{}
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		trs.upgrades.Add(1)
		trs.open.Add(1)
		defer trs.open.Add(-1)
		defer conn.Close()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})}

	go httpServer.Serve(listener)
	t.Cleanup(func() {
		httpServer.Close()
		listener.Close()
	})

	trs.server = &httptest.Server{Listener: listener}
	return trs
}

func TestRealRelayRemovedWhileEstablishingLeavesNoHalfConnection(t *testing.T) {
	s := newRealTestSfu(t)

	// A server which accepts TCP connections but never completes the websocket
	// handshake, so the relay stays in the "establishing" phase.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := listener.Addr().String()

	var (
		accepted   atomic.Int32
		mu         sync.Mutex
		heldConns  []net.Conn
		acceptDone = make(chan struct{})
	)

	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			mu.Lock()
			heldConns = append(heldConns, conn)
			mu.Unlock()
		}
	}()

	t.Cleanup(func() {
		listener.Close()
		<-acceptDone
		mu.Lock()
		for _, c := range heldConns {
			c.Close()
		}
		mu.Unlock()
	})

	url := wsURLFor(addr)
	submitServers(s, url)

	if !waitFor(5*time.Second, func() bool { return accepted.Load() == 1 }) {
		t.Fatalf("expected the relay to start dialling the trunk address")
	}

	// Remove while the handshake is still in flight.
	submitServers(s)

	status := waitForStatus(t, s, 10*time.Second, func(st *SFUStatus) bool {
		return len(st.Servers) == 0 && len(st.Clients) == 0
	})
	if len(status.Servers) != 0 || len(status.Clients) != 0 {
		t.Fatalf("expected no half connected relay to remain, got %+v", status)
	}

	// The aborted handshake must have been released rather than left open.
	if !waitFor(10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range heldConns {
			// A closed peer connection surfaces as a read error / EOF quickly.
			c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			buf := make([]byte, 1)
			_, err := c.Read(buf)
			if err == nil {
				return false
			}
			if strings.Contains(err.Error(), "timeout") {
				return false
			}
		}
		return true
	}) {
		// Not fatal on its own; the important assertion is that the relay was
		// stopped. Report it so a regression is visible.
		t.Fatalf("expected the in-flight connection to be released when the relay was removed")
	}

	// A subsequent submission of the same address must dial again.
	submitServers(s, url)
	if !waitFor(10*time.Second, func() bool { return accepted.Load() >= 2 }) {
		t.Fatalf("expected a fresh dial after re-submitting the address")
	}
}
