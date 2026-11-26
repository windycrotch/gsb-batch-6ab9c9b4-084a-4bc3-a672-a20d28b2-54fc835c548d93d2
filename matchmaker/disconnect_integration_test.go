package matchmaking_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/catalinfl/adler"
	matchmaking "github.com/catalinfl/adler/matchmaker"
)

// queueTracker records disconnect callbacks registered by application code.
type queueTracker struct {
	mu           sync.Mutex
	disconnected []*adler.Session
	signal       chan struct{}
}

func newQueueTracker() *queueTracker {
	return &queueTracker{signal: make(chan struct{}, 16)}
}

func (q *queueTracker) handle(s *adler.Session) {
	q.mu.Lock()
	q.disconnected = append(q.disconnected, s)
	q.mu.Unlock()

	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func (q *queueTracker) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.disconnected)
}

// setupDisconnectMatchmakerServer starts a server whose connect hook enqueues
// every session and whose application-level disconnect handler is registered
// through HandleDisconnect, so the test can prove the matchmaker hook is additive.
func setupDisconnectMatchmakerServer(
	t *testing.T,
	tracker *queueTracker,
	opts ...matchmaking.MatchmakingOption,
) (*adler.Adler, *httptest.Server, chan *adler.Session) {
	t.Helper()

	a := adler.New(
		adler.WithDispatchAsync(true),
		adler.WithMessageBufferSize(16),
	)
	mm := matchmaking.NewMatchmaker(a, opts...)

	// Registered after the matchmaker on purpose: HandleDisconnect replaces its
	// own callback slot, and must not remove matchmaker cleanup.
	a.HandleDisconnect(tracker.handle)

	sessions := make(chan *adler.Session, 16)
	a.HandleConnect(func(s *adler.Session) {
		if err := mm.AddToQueue(s); err != nil {
			t.Errorf("add to queue: %v", err)
		}
		sessions <- s
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = a.HandleRequest(w, r)
	}))
	t.Cleanup(srv.Close)

	return a, srv, sessions
}

func waitDisconnect(t *testing.T, tracker *queueTracker, want int) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for tracker.count() < want {
		select {
		case <-tracker.signal:
		case <-deadline:
			t.Fatalf("disconnect handler called %d times, want %d", tracker.count(), want)
		}
	}
}

func waitEventOfType(t *testing.T, c *wsClient, want string) queueEvent {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			t.Fatalf("timed out waiting for %s event", want)
		}
		evt := waitQueueEvent(t, c)
		switch {
		case want == "promoted" && evt.Payload.PromotedToQueue != nil:
			return evt
		case want == "match" && evt.Payload.MatchFound != nil:
			return evt
		}
	}
}

func TestDisconnectRemovesSessionFromWaitingQueue(t *testing.T) {
	tracker := newQueueTracker()
	_, srv, sessions := setupDisconnectMatchmakerServer(t, tracker,
		matchmaking.WithRoomSize(10),
		matchmaking.WithQueueLength(1),
	)

	// First player occupies the only main-queue slot.
	first := dialWS(t, wsURL(srv.URL)+"?cid=1")
	defer first.Close()
	_ = waitSession(t, sessions, "missing first session")
	if evt := waitQueueEvent(t, first); evt.Payload.QueueJoined == nil {
		t.Fatalf("first player expected QueueJoined, got %#v", evt)
	}

	// Second player spills into the waiting queue, then disconnects.
	second := dialWS(t, wsURL(srv.URL)+"?cid=2")
	_ = waitSession(t, sessions, "missing second session")
	if evt := waitQueueEvent(t, second); evt.Payload.WaitQueueJoined == nil {
		t.Fatalf("second player expected WaitQueueJoined, got %#v", evt)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("disconnect second player: %v", err)
	}
	waitDisconnect(t, tracker, 1)

	// Give the worker time to process the disconnect cleanup, then verify a new
	// player takes the waiting slot rather than finding a stale entry.
	time.Sleep(100 * time.Millisecond)

	third := dialWS(t, wsURL(srv.URL)+"?cid=3")
	defer third.Close()
	_ = waitSession(t, sessions, "missing third session")

	evt := waitQueueEvent(t, third)
	if evt.Payload.WaitQueueJoined == nil {
		t.Fatalf("third player expected WaitQueueJoined after cleanup, got %#v", evt)
	}
}

func TestDisconnectFromMainQueuePromotesNextWaiter(t *testing.T) {
	tracker := newQueueTracker()
	_, srv, sessions := setupDisconnectMatchmakerServer(t, tracker,
		matchmaking.WithRoomSize(3),
		matchmaking.WithQueueLength(2),
	)

	connA := dialWS(t, wsURL(srv.URL)+"?cid=a")
	defer connA.Close()
	_ = waitSession(t, sessions, "missing session a")

	connB := dialWS(t, wsURL(srv.URL)+"?cid=b")
	defer connB.Close()
	_ = waitSession(t, sessions, "missing session b")

	connC := dialWS(t, wsURL(srv.URL)+"?cid=c")
	defer connC.Close()
	_ = waitSession(t, sessions, "missing session c")

	if evt := waitQueueEvent(t, connA); evt.Payload.QueueJoined == nil {
		t.Fatalf("player a expected QueueJoined, got %#v", evt)
	}
	if evt := waitQueueEvent(t, connB); evt.Payload.QueueJoined == nil {
		t.Fatalf("player b expected QueueJoined, got %#v", evt)
	}
	if evt := waitQueueEvent(t, connC); evt.Payload.WaitQueueJoined == nil {
		t.Fatalf("player c expected WaitQueueJoined, got %#v", evt)
	}

	// Player a disconnects; its slot must go to the waiting player c.
	if err := connA.Close(); err != nil {
		t.Fatalf("disconnect player a: %v", err)
	}
	waitDisconnect(t, tracker, 1)

	if evt := waitEventOfType(t, connC, "promoted"); evt.Payload.PromotedToQueue == nil {
		t.Fatalf("player c expected PromotedToQueue, got %#v", evt)
	}
}

func TestDisconnectedWaiterIsSkippedOnPromotion(t *testing.T) {
	tracker := newQueueTracker()
	_, srv, sessions := setupDisconnectMatchmakerServer(t, tracker,
		matchmaking.WithRoomSize(3),
		matchmaking.WithQueueLength(2),
	)

	dial := func(cid string) *wsClient {
		c := dialWS(t, wsURL(srv.URL)+"?cid="+cid)
		_ = waitSession(t, sessions, "missing session "+cid)
		return c
	}

	connA := dial("a")
	defer connA.Close()
	connB := dial("b")
	defer connB.Close()
	connC := dial("c")
	connD := dial("d")
	defer connD.Close()

	if evt := waitQueueEvent(t, connA); evt.Payload.QueueJoined == nil {
		t.Fatalf("player a expected QueueJoined, got %#v", evt)
	}
	if evt := waitQueueEvent(t, connB); evt.Payload.QueueJoined == nil {
		t.Fatalf("player b expected QueueJoined, got %#v", evt)
	}
	if evt := waitQueueEvent(t, connC); evt.Payload.WaitQueueJoined == nil {
		t.Fatalf("player c expected WaitQueueJoined, got %#v", evt)
	}
	if evt := waitQueueEvent(t, connD); evt.Payload.WaitQueueJoined == nil {
		t.Fatalf("player d expected WaitQueueJoined, got %#v", evt)
	}

	// The first waiter disconnects. It must not be promoted, and the freed main
	// slot must go to the next waiter (d).
	if err := connC.Close(); err != nil {
		t.Fatalf("disconnect player c: %v", err)
	}
	waitDisconnect(t, tracker, 1)
	time.Sleep(100 * time.Millisecond)

	if err := connA.Close(); err != nil {
		t.Fatalf("disconnect player a: %v", err)
	}
	waitDisconnect(t, tracker, 2)

	if evt := waitEventOfType(t, connD, "promoted"); evt.Payload.PromotedToQueue == nil {
		t.Fatalf("player d expected PromotedToQueue, got %#v", evt)
	}
}

func TestDisconnectMidQueueDoesNotCreateGhostRoom(t *testing.T) {
	tracker := newQueueTracker()
	a, srv, sessions := setupDisconnectMatchmakerServer(t, tracker,
		matchmaking.WithRoomSize(4),
		matchmaking.WithQueueLength(4),
	)

	dial := func(cid string) *wsClient {
		c := dialWS(t, wsURL(srv.URL)+"?cid="+cid)
		_ = waitSession(t, sessions, "missing session "+cid)
		return c
	}

	connA := dial("a")
	defer connA.Close()
	connB := dial("b")
	defer connB.Close()
	connC := dial("c")
	defer connC.Close()

	for _, c := range []*wsClient{connA, connB, connC} {
		if evt := waitQueueEvent(t, c); evt.Payload.QueueJoined == nil {
			t.Fatalf("expected QueueJoined, got %#v", evt)
		}
	}

	// Player c disconnects while queued in the main queue. A ghost entry here
	// would make the next room contain a dead slot.
	if err := connC.Close(); err != nil {
		t.Fatalf("disconnect player c: %v", err)
	}
	waitDisconnect(t, tracker, 1)
	time.Sleep(100 * time.Millisecond)

	// Two fresh players complete the room with the two live queued players.
	connD := dial("d")
	defer connD.Close()
	connE := dial("e")
	defer connE.Close()

	members := map[string]string{}
	for _, tc := range []struct {
		name string
		c    *wsClient
	}{
		{name: "a", c: connA},
		{name: "b", c: connB},
		{name: "d", c: connD},
		{name: "e", c: connE},
	} {
		evt := waitEventOfType(t, tc.c, "match")
		if evt.Payload.MatchFound.Players != 4 {
			t.Fatalf("player %s match_found players: got=%d want=4", tc.name, evt.Payload.MatchFound.Players)
		}
		members[tc.name] = evt.Payload.MatchFound.RoomId
	}

	roomID := members["a"]
	if roomID == "" || members["b"] != roomID || members["d"] != roomID || members["e"] != roomID {
		t.Fatalf("players were not matched into the same room: %v", members)
	}

	room, err := a.Room(roomID)
	if err != nil {
		t.Fatalf("room lookup failed: %v", err)
	}
	if got := room.Len(); got != 4 {
		t.Fatalf("room size: got=%d want=4 (notified players=4)", got)
	}
	for _, member := range room.Sessions() {
		if member.IsClosed() {
			t.Fatal("closed session was placed in a room")
		}
	}
}
