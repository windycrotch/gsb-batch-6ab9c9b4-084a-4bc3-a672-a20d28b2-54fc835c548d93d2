package matchmaking

import (
	"container/list"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/catalinfl/adler"
	"github.com/lxzan/gws"
)

// newQueueOnlyMatchmaker builds a matchmaker without starting the worker
// goroutine, so tests can drive queue transitions synchronously.
func newQueueOnlyMatchmaker(t *testing.T, opts ...MatchmakingOption) *Matchmaker {
	t.Helper()

	cfg := newMatchmakingConfig(opts...)
	m := &Matchmaker{
		commands:           make(chan matchmakingCommand, cfg.CommandBuffer),
		waitQueue:          list.New(),
		queue:              list.New(),
		inWaitQueueMap:     map[*adler.Session]*list.Element{},
		inQueueMap:         map[*adler.Session]*list.Element{},
		adler:              adler.New(),
		maxQueue:           cfg.MaxQueue,
		roomSize:           cfg.RoomSize,
		minRoomSize:        cfg.MinRoomSize,
		partialRoomTimeout: cfg.PartialRoomTimeout,
		pendingRemovals:    map[*adler.Session]struct{}{},
		retrySignal:        make(chan struct{}, cfg.CommandBuffer),
	}
	return m
}

// queueSessions returns the sessions currently held by a list in order.
func queueSessions(l *list.List) []*adler.Session {
	sessions := make([]*adler.Session, 0, l.Len())
	for element := l.Front(); element != nil; element = element.Next() {
		sessions = append(sessions, element.Value.(*queueItem).session)
	}
	return sessions
}

// assertQueueState checks that the list, its index and the session status all
// describe the same queue membership.
func assertQueueState(t *testing.T, l *list.List, index map[*adler.Session]*list.Element, wantStatus string) {
	t.Helper()

	if len(index) != l.Len() {
		t.Fatalf("index/list size mismatch: index=%d list=%d", len(index), l.Len())
	}

	for element := l.Front(); element != nil; element = element.Next() {
		item, ok := element.Value.(*queueItem)
		if !ok || item == nil || item.session == nil {
			t.Fatal("queue contains a malformed item")
		}
		if index[item.session] != element {
			t.Fatal("queue index does not point at the queued element")
		}
		if status, _ := item.session.GetString(sessionKeyQueueStatus); status != wantStatus {
			t.Fatalf("session status: got=%q want=%q", status, wantStatus)
		}
	}
}

// newSocketSession dials a real websocket connection against a test server and
// returns the captured server-side session. Unless the connection is closed by
// the caller, it stays open and the session stays usable for queue operations.
func newSocketSession(t *testing.T) (*adler.Session, *gws.Conn) {
	t.Helper()

	a := adler.New(adler.WithDispatchAsync(false))
	sessions := make(chan *adler.Session, 1)
	a.HandleConnect(func(s *adler.Session) { sessions <- s })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = a.HandleRequest(w, r)
	}))

	conn, _, err := gws.NewClient(gws.BuiltinEventHandler{}, &gws.ClientOption{Addr: wsAddress(srv.URL)})
	if err != nil {
		srv.Close()
		t.Fatalf("dial websocket: %v", err)
	}
	go conn.ReadLoop()

	t.Cleanup(func() {
		_ = conn.NetConn().Close()
		srv.Close()
	})

	select {
	case s := <-sessions:
		return s, conn
	case <-time.After(2 * time.Second):
		t.Fatal("session was not captured")
		return nil, nil
	}
}

// newLiveSession returns a connected session that stays usable during the test.
func newLiveSession(t *testing.T) *adler.Session {
	t.Helper()
	s, _ := newSocketSession(t)
	return s
}

// newClosedSession creates a real websocket session, disconnects it, and waits
// until the server has marked it closed.
func newClosedSession(t *testing.T) *adler.Session {
	t.Helper()

	a := adler.New(adler.WithDispatchAsync(false))
	sessions := make(chan *adler.Session, 1)
	disconnected := make(chan struct{}, 1)
	a.HandleConnect(func(s *adler.Session) { sessions <- s })
	a.HandleDisconnect(func(*adler.Session) { disconnected <- struct{}{} })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = a.HandleRequest(w, r)
	}))
	defer srv.Close()

	conn, _, err := gws.NewClient(gws.BuiltinEventHandler{}, &gws.ClientOption{Addr: wsAddress(srv.URL)})
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	go conn.ReadLoop()

	var s *adler.Session
	select {
	case s = <-sessions:
	case <-time.After(2 * time.Second):
		t.Fatal("session was not captured")
	}

	_ = conn.NetConn().Close()
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("session was not disconnected")
	}

	if !s.IsClosed() {
		t.Fatal("disconnected session is not marked closed")
	}
	return s
}

func wsAddress(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func TestPromotionSkipsClosedSessionAndFillsSlotWithNextWaiter(t *testing.T) {
	m := newQueueOnlyMatchmaker(t,
		WithRoomSize(10),
		WithQueueLength(2),
	)

	live1 := newLiveSession(t)
	live2 := newLiveSession(t)
	gone := newClosedSession(t)
	live3 := newLiveSession(t)

	now := time.Now()
	m.addToMainQueue(live1, now)
	m.addToMainQueue(live2, now)
	m.addToWaitingQueue(gone, now)
	m.addToWaitingQueue(live3, now)

	// Free one main-queue slot; the disconnected waiter must be skipped and the
	// slot must go to the next live waiter.
	m.handleRemoveFromQueue(live1, now)

	if got := queueSessions(m.waitQueue); len(got) != 0 {
		t.Fatalf("waiting queue not drained: got=%d want=0", len(got))
	}
	assertQueueState(t, m.queue, m.inQueueMap, queueStatusQueued)

	if got := queueSessions(m.queue); len(got) != 2 || got[0] != live2 || got[1] != live3 {
		t.Fatalf("unexpected main queue order: got=%v", got)
	}

	if status, _ := gone.GetString(sessionKeyQueueStatus); status != queueStatusLeft {
		t.Fatalf("closed waiter status: got=%q want=%q", status, queueStatusLeft)
	}

	if _, exists := m.inQueueMap[gone]; exists {
		t.Fatal("closed waiter still occupies a main queue slot")
	}
}

func TestPromotionUpdatesQueueIndexAndStatusTogether(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(10), WithQueueLength(1))

	main := newLiveSession(t)
	waiter := newLiveSession(t)

	now := time.Now()
	m.addToMainQueue(main, now)
	m.addToWaitingQueue(waiter, now)
	m.handleRemoveFromQueue(main, now)

	if got := queueSessions(m.queue); len(got) != 1 || got[0] != waiter {
		t.Fatalf("unexpected main queue after promotion: %#v", got)
	}
	assertQueueState(t, m.queue, m.inQueueMap, queueStatusQueued)

	if m.inWaitQueueMap[waiter] != nil {
		t.Fatal("promoted session still has a waiting-queue index entry")
	}
	if m.waitQueue.Len() != 0 {
		t.Fatalf("waiting queue not empty: got=%d", m.waitQueue.Len())
	}
	if status, _ := waiter.GetString(sessionKeyQueueStatus); status != queueStatusQueued {
		t.Fatalf("promoted session status: got=%q want=%q", status, queueStatusQueued)
	}
}

func TestCreateRoomSkipsClosedSessionsAndReportsRealMemberCount(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(2))

	gone := newClosedSession(t)
	live1 := newLiveSession(t)
	live2 := newLiveSession(t)

	now := time.Now()
	m.addToMainQueue(gone, now)
	m.addToMainQueue(live1, now)
	m.addToMainQueue(live2, now)

	m.processQueueTransitions(now)

	if m.queue.Len() != 0 || len(m.inQueueMap) != 0 {
		t.Fatalf("main queue not drained: list=%d index=%d", m.queue.Len(), len(m.inQueueMap))
	}
	if _, exists := m.inQueueMap[gone]; exists {
		t.Fatal("closed session still indexed in the main queue")
	}

	room := live1.Room()
	if room == nil || room != live2.Room() {
		t.Fatal("live sessions were not matched into the same room")
	}
	if got := room.Len(); got != 2 {
		t.Fatalf("room size: got=%d want=2", got)
	}
	for _, member := range room.Sessions() {
		if member == gone {
			t.Fatal("closed session was added to the room")
		}
		if status, _ := member.GetString(sessionKeyQueueStatus); status != queueStatusPlaying {
			t.Fatalf("room member status: got=%q want=%q", status, queueStatusPlaying)
		}
	}
	if status, _ := gone.GetString(sessionKeyQueueStatus); status != queueStatusLeft {
		t.Fatalf("closed session status: got=%q want=%q", status, queueStatusLeft)
	}
}

func TestCreateRoomDoesNotDropBelowMinimumSize(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(3), WithMinRoomSize(3))

	gone := newClosedSession(t)
	live := newLiveSession(t)

	now := time.Now()
	m.addToMainQueue(gone, now)
	m.addToMainQueue(live, now)

	m.processQueueTransitions(now)

	if m.queue.Len() != 1 || m.inQueueMap[live] == nil {
		t.Fatalf("surviving session was lost: list=%d index=%d", m.queue.Len(), len(m.inQueueMap))
	}
	if live.Room() != nil {
		t.Fatal("room was created below the minimum size")
	}
	if status, _ := live.GetString(sessionKeyQueueStatus); status != queueStatusQueued {
		t.Fatalf("surviving session status: got=%q want=%q", status, queueStatusQueued)
	}
}

func TestRemoveKeepsRelativeOrderOfOtherSessions(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(10))

	s1 := newLiveSession(t)
	s2 := newLiveSession(t)
	s3 := newLiveSession(t)

	now := time.Now()
	m.addToMainQueue(s1, now)
	m.addToMainQueue(s2, now)
	m.addToMainQueue(s3, now)

	m.handleRemoveFromQueue(s2, now)

	got := queueSessions(m.queue)
	if len(got) != 2 || got[0] != s1 || got[1] != s3 {
		t.Fatalf("relative order changed after removal: %v", got)
	}
	assertQueueState(t, m.queue, m.inQueueMap, queueStatusQueued)

	// Repeated cleanup must be a safe no-op.
	m.handleRemoveFromQueue(s2, now)
	if status, _ := s2.GetString(sessionKeyQueueStatus); status != queueStatusLeft {
		t.Fatalf("removed session status: got=%q want=%q", status, queueStatusLeft)
	}
	if got := queueSessions(m.queue); len(got) != 2 || got[0] != s1 || got[1] != s3 {
		t.Fatalf("repeated removal changed the queue: %v", got)
	}
}

func TestCancelAndDisconnectBothLeaveSessionRemoved(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(10), WithQueueLength(1))

	main := newLiveSession(t)
	waiter := newClosedSession(t)

	now := time.Now()
	m.addToMainQueue(main, now)
	m.addToWaitingQueue(waiter, now)

	// A cancel request and the disconnect cleanup can both arrive; either order
	// must end with the session removed and marked as left.
	m.handleRemoveFromQueue(waiter, now) // simulated disconnect cleanup
	m.handleRemoveFromQueue(waiter, now) // simulated duplicate/cancel

	if m.waitQueue.Len() != 0 || len(m.inWaitQueueMap) != 0 {
		t.Fatalf("waiting queue retained the session: list=%d index=%d", m.waitQueue.Len(), len(m.inWaitQueueMap))
	}
	if status, _ := waiter.GetString(sessionKeyQueueStatus); status != queueStatusLeft {
		t.Fatalf("session status: got=%q want=%q", status, queueStatusLeft)
	}
}

func TestBusyCommandBufferSchedulesRemovalRetry(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(10), WithCommandBuffer(1))

	s := newLiveSession(t)
	now := time.Now()
	m.addToWaitingQueue(s, now)

	// Saturate the shared command buffer so the disconnect cleanup cannot be
	// queued as a command.
	m.commands <- matchmakingCommand{kind: matchmakingCommandAdd, session: s}

	m.handleDisconnect(s)

	m.retryMu.Lock()
	pending := len(m.pendingRemovals)
	m.retryMu.Unlock()
	if pending != 1 {
		t.Fatalf("failed removal was not scheduled for retry: pending=%d", pending)
	}
	if m.inWaitQueueMap[s] == nil {
		t.Fatal("removal ran before the retry was drained")
	}

	// The worker drains the retry and applies the removal even though the
	// command buffer is still full.
	m.drainPendingRemovals()

	if m.inWaitQueueMap[s] != nil || m.waitQueue.Len() != 0 {
		t.Fatal("retried removal did not clear the waiting queue")
	}
	if status, _ := s.GetString(sessionKeyQueueStatus); status != queueStatusLeft {
		t.Fatalf("session status: got=%q want=%q", status, queueStatusLeft)
	}

	// Retrying again must be safe.
	m.drainPendingRemovals()
}

func TestAddUsesSlotFreedByDeferredClosedSession(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(10), WithQueueLength(1))

	gone := newClosedSession(t)
	// Simulate the state right after a disconnect whose removal command was
	// rejected: the closed session still occupies the only main-queue slot.
	m.addToMainQueue(gone, time.Now())

	live := newLiveSession(t)
	m.handleAddToQueue(live, time.Now())

	if m.inWaitQueueMap[live] != nil {
		t.Fatal("live session was pushed to the waiting queue behind a closed entry")
	}
	if got := queueSessions(m.queue); len(got) != 1 || got[0] != live {
		t.Fatalf("main queue: got=%v want=[live]", got)
	}
	assertQueueState(t, m.queue, m.inQueueMap, queueStatusQueued)
	if status, _ := gone.GetString(sessionKeyQueueStatus); status != queueStatusLeft {
		t.Fatalf("closed session status: got=%q want=%q", status, queueStatusLeft)
	}
}

func TestAddAfterDisconnectDoesNotRequeueClosedSession(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(10))

	gone := newClosedSession(t)
	m.handleAddToQueue(gone, time.Now())

	if m.queue.Len() != 0 || m.waitQueue.Len() != 0 {
		t.Fatalf("closed session entered a queue: main=%d wait=%d", m.queue.Len(), m.waitQueue.Len())
	}
	if status, _ := gone.GetString(sessionKeyQueueStatus); status == queueStatusQueued || status == queueStatusWaiting {
		t.Fatalf("closed session marked as queued: %q", status)
	}
}

func TestStaleIndexEntryIsPrunedWithoutDisturbingOthers(t *testing.T) {
	m := newQueueOnlyMatchmaker(t, WithRoomSize(10))

	s1 := newLiveSession(t)
	s2 := newLiveSession(t)
	s3 := newLiveSession(t)

	now := time.Now()
	m.addToMainQueue(s1, now)
	m.addToMainQueue(s2, now)
	m.addToMainQueue(s3, now)

	// Simulate a corrupted/duplicated index entry: the map points somewhere
	// other than the list element that actually holds s2.
	m.inQueueMap[s2] = m.inQueueMap[s1]

	m.pruneInvalidQueueEntries()

	// s1 keeps its legitimate entry (the index points at it), while s2's now
	// unindexed list entry and s3 are re-derived from the list itself.
	if _, exists := m.inQueueMap[s2]; exists {
		t.Fatal("stale index entry survived pruning")
	}
	got := queueSessions(m.queue)
	if len(got) == 0 || got[0] != s1 {
		t.Fatalf("pruning disturbed the queue front: %v", got)
	}
	assertQueueState(t, m.queue, m.inQueueMap, queueStatusQueued)
}

func TestPromotionRejectsDuplicateIndexedSession(t *testing.T) {
	// maxQueue must exceed the main queue length, otherwise promotion is not
	// attempted at all and the duplicate would never be exercised.
	m := newQueueOnlyMatchmaker(t, WithRoomSize(10), WithQueueLength(2))

	live := newLiveSession(t)
	now := time.Now()
	m.addToMainQueue(live, now)
	// Duplicate tracking: the same session also sits in the waiting queue.
	m.addToWaitingQueue(live, now)

	m.promoteFromWaitingQueue(now)

	if m.waitQueue.Len() != 0 {
		t.Fatalf("waiting queue not drained: got=%d", m.waitQueue.Len())
	}
	if got := queueSessions(m.queue); len(got) != 1 || got[0] != live {
		t.Fatalf("duplicate promotion changed the main queue: %v", got)
	}
	assertQueueState(t, m.queue, m.inQueueMap, queueStatusQueued)
}
