package matchmaking_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/catalinfl/adler"
	matchmaking "github.com/catalinfl/adler/matchmaker"
	"github.com/lxzan/gws"
	"google.golang.org/protobuf/proto"
)

type queueEvent struct {
	Payload struct {
		QueueJoined     *matchmaking.QueueJoined     `json:"QueueJoined"`
		WaitQueueJoined *matchmaking.WaitQueueJoined `json:"WaitQueueJoined"`
		PromotedToQueue *matchmaking.PromotedToQueue `json:"PromotedToQueue"`
		MatchFound      *matchmaking.MatchFound      `json:"MatchFound"`
		QueueError      *matchmaking.QueueError      `json:"QueueError"`
	} `json:"Payload"`
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func waitSession(t *testing.T, ch <-chan *adler.Session, msg string) *adler.Session {
	t.Helper()

	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
		return nil
	}
}

// testTimeoutError is returned when a channel-based read times out.
type testTimeoutError struct{}

func (e *testTimeoutError) Error() string { return "i/o timeout" }

// testEventHandler collects incoming messages and signals connection close.
type testEventHandler struct {
	msgs chan *gws.Message
	err  error
	errc chan struct{}
	once sync.Once
}

func (h *testEventHandler) OnOpen(socket *gws.Conn) {}
func (h *testEventHandler) OnClose(socket *gws.Conn, err error) {
	h.once.Do(func() {
		h.err = err
		close(h.errc)
	})
}
func (h *testEventHandler) OnPing(socket *gws.Conn, payload []byte) {
	_ = socket.WritePong(payload)
}
func (h *testEventHandler) OnPong(socket *gws.Conn, payload []byte) {}
func (h *testEventHandler) OnMessage(socket *gws.Conn, message *gws.Message) {
	h.msgs <- message
}

// wsClient wraps a gws client connection with a background reader goroutine.
type wsClient struct {
	conn    *gws.Conn
	handler *testEventHandler
}

func dialWS(t *testing.T, url string) *wsClient {
	t.Helper()
	handler := &testEventHandler{
		msgs: make(chan *gws.Message, 64),
		errc: make(chan struct{}),
	}
	conn, _, err := gws.NewClient(handler, &gws.ClientOption{Addr: url})
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	go conn.ReadLoop()
	return &wsClient{conn: conn, handler: handler}
}

func (c *wsClient) Close() error {
	return c.conn.NetConn().Close()
}

func (c *wsClient) readServerMessage(timeout time.Duration) (gws.Opcode, []byte, error) {
	select {
	case msg := <-c.handler.msgs:
		op := msg.Opcode
		payload := make([]byte, len(msg.Bytes()))
		copy(payload, msg.Bytes())
		msg.Close()
		return op, payload, nil
	case <-c.handler.errc:
		return 0, nil, c.handler.err
	case <-time.After(timeout):
		return 0, nil, &testTimeoutError{}
	}
}

func waitQueueEvent(t *testing.T, c *wsClient) queueEvent {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			t.Fatal("timed out waiting for queue event")
		}

		op, payload, err := c.readServerMessage(timeout)
		if err != nil {
			t.Fatalf("read server message: %v", err)
		}
		if op != gws.OpcodeText {
			continue
		}

		var event queueEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			t.Fatalf("unmarshal server event: %v", err)
		}

		return event
	}
}

func waitMatchRoomID(t *testing.T, c *wsClient) string {
	t.Helper()

	first := waitQueueEvent(t, c)
	if first.Payload.QueueJoined == nil {
		t.Fatalf("unexpected first event: %#v", first)
	}

	second := waitQueueEvent(t, c)
	if second.Payload.MatchFound == nil {
		t.Fatalf("unexpected second event: %#v", second)
	}
	if second.Payload.MatchFound.RoomId == "" {
		t.Fatal("match_found event did not include a room_id")
	}

	return second.Payload.MatchFound.RoomId
}

func setupMatchmakerServer(t *testing.T, roomSize int) (*adler.Adler, *httptest.Server, chan *adler.Session) {
	t.Helper()

	a := adler.New(
		adler.WithDispatchAsync(true),
		adler.WithMessageBufferSize(8),
	)
	mm := matchmaking.NewMatchmaker(a, matchmaking.WithRoomSize(roomSize))
	sessions := make(chan *adler.Session, 8)

	a.HandleConnect(func(s *adler.Session) {
		mm.AddToQueue(s)
		sessions <- s
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = a.HandleRequest(w, r)
	}))

	t.Cleanup(srv.Close)

	return a, srv, sessions
}

func TestMatchmakerCreatesRoomWithRealConnections(t *testing.T) {
	a, srv, sessions := setupMatchmakerServer(t, 2)

	conn1 := dialWS(t, wsURL(srv.URL)+"?cid=1")
	defer conn1.Close()

	conn2 := dialWS(t, wsURL(srv.URL)+"?cid=2")
	defer conn2.Close()

	s1 := waitSession(t, sessions, "missing first session")
	s2 := waitSession(t, sessions, "missing second session")

	roomID1 := waitMatchRoomID(t, conn1)
	roomID2 := waitMatchRoomID(t, conn2)

	if roomID1 != roomID2 {
		t.Fatalf("players were matched into different rooms: %q vs %q", roomID1, roomID2)
	}

	room, err := a.Room(roomID1)
	if err != nil {
		t.Fatalf("room lookup failed: %v", err)
	}

	if got := room.Len(); got != 2 {
		t.Fatalf("unexpected room size: got=%d want=2", got)
	}

	if got := s1.Room(); got != room {
		t.Fatalf("first session room mismatch: got=%p want=%p", got, room)
	}
	if got := s2.Room(); got != room {
		t.Fatalf("second session room mismatch: got=%p want=%p", got, room)
	}

	if got := room.Name(); got != roomID1 {
		t.Fatalf("unexpected room name: got=%q want=%q", got, roomID1)
	}
}

func TestMatchmakerCreatesSeparateRoomsForMultiplePairs(t *testing.T) {
	a, srv, sessions := setupMatchmakerServer(t, 2)

	conns := make([]*wsClient, 0, 4)
	for i := 1; i <= 4; i++ {
		c := dialWS(t, wsURL(srv.URL)+"?cid="+fmt.Sprintf("%d", i))
		conns = append(conns, c)
		defer c.Close()
	}

	captured := make([]*adler.Session, 0, 4)
	for range 4 {
		captured = append(captured, waitSession(t, sessions, "missing connected session"))
	}

	roomIDs := make([]string, 0, 4)
	for _, c := range conns {
		roomIDs = append(roomIDs, waitMatchRoomID(t, c))
	}

	if roomIDs[0] != roomIDs[1] {
		t.Fatalf("first pair matched into different rooms: %q vs %q", roomIDs[0], roomIDs[1])
	}
	if roomIDs[2] != roomIDs[3] {
		t.Fatalf("second pair matched into different rooms: %q vs %q", roomIDs[2], roomIDs[3])
	}
	if roomIDs[0] == roomIDs[2] {
		t.Fatalf("expected two separate rooms, both pairs got %q", roomIDs[0])
	}

	room1, err := a.Room(roomIDs[0])
	if err != nil {
		t.Fatalf("room 1 lookup failed: %v", err)
	}
	room2, err := a.Room(roomIDs[2])
	if err != nil {
		t.Fatalf("room 2 lookup failed: %v", err)
	}

	if got := room1.Len(); got != 2 {
		t.Fatalf("unexpected first room size: got=%d want=2", got)
	}
	if got := room2.Len(); got != 2 {
		t.Fatalf("unexpected second room size: got=%d want=2", got)
	}

	if got := captured[0].Room(); got == nil || got.Name() != roomIDs[0] {
		t.Fatalf("first captured session is not in the expected room: got=%v room=%q", got, roomIDs[0])
	}
	if got := captured[2].Room(); got == nil || got.Name() != roomIDs[2] {
		t.Fatalf("third captured session is not in the expected room: got=%v room=%q", got, roomIDs[2])
	}
}

func TestClosureBugPromotedPlayers(t *testing.T) {
	a := adler.New(
		adler.WithDispatchAsync(false), // Sync mode pentru debugging
		adler.WithMessageBufferSize(32),
	)
	mm := matchmaking.NewMatchmaker(
		a,
		matchmaking.WithRoomSize(10),
		matchmaking.WithQueueLength(2),
	)

	sessions := make(chan *adler.Session, 10)
	a.HandleConnect(func(s *adler.Session) {
		cid := s.Request().URL.Query().Get("cid")
		s.Set("cid", cid)
		mm.AddToQueue(s)
		sessions <- s
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = a.HandleRequest(w, r)
	}))
	defer srv.Close()

	// Conectez 5 playeri
	conns := make([]*wsClient, 0, 5)
	for i := 1; i <= 5; i++ {
		c := dialWS(t, wsURL(srv.URL)+"?cid="+fmt.Sprintf("%d", i))
		conns = append(conns, c)
		defer c.Close()
	}

	// Consume initial queue notifications so we can assert only promotion events later.
	for i := 0; i < 5; i++ {
		evt := waitQueueEvent(t, conns[i])
		if i < 2 {
			if evt.Payload.QueueJoined == nil {
				t.Fatalf("player %d expected QueueJoined, got %#v", i+1, evt)
			}
			continue
		}

		if evt.Payload.WaitQueueJoined == nil {
			t.Fatalf("player %d expected WaitQueueJoined, got %#v", i+1, evt)
		}
	}

	captured := make([]*adler.Session, 0, 5)
	for i := 0; i < 5; i++ {
		s := waitSession(t, sessions, fmt.Sprintf("missing session %d", i+1))
		captured = append(captured, s)
		status, _ := s.GetString("queue_status")
		t.Logf("Player %d status: %s", i+1, status)
	}

	t.Log("Calling RemoveFromQueue(p1)...")
	if err := mm.RemoveFromQueue(captured[0]); err != nil {
		t.Fatalf("remove player from queue: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // Give time for notifications

	// Removing one main-queue player opens exactly one slot, so only p3 is promoted.
	t.Log("Waiting for promoted_to_queue on player 3 (conns[2])...")
	evt := waitQueueEvent(t, conns[2])
	t.Logf("Player 3 received: %#v", evt)
	if evt.Payload.PromotedToQueue == nil {
		t.Fatalf("player 3 expected PromotedToQueue, got %#v", evt)
	}

	t.Log("Promotion flow works: waiting player receives promoted_to_queue on correct connection")
}

func TestMatchmakerEightPlayersOneSecondInterval(t *testing.T) {
	_, srv, sessions := setupMatchmakerServer(t, 2) // room size = 2 => 4 rooms for 8 players

	conns := make([]*wsClient, 0, 8)
	for i := 1; i <= 8; i++ {
		c := dialWS(t, wsURL(srv.URL)+"?cid="+fmt.Sprintf("%d", i))
		conns = append(conns, c)
		t.Logf("Dialed client %d", i)
		time.Sleep(1 * time.Second) // 1 second interval between connects
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	// Collect room IDs reported to each client and print them
	roomIDs := make([]string, 0, len(conns))
	unique := map[string]struct{}{}
	for i, c := range conns {
		rid := waitMatchRoomID(t, c)
		t.Logf("Client %d received match_found -> room_id: %s", i+1, rid)
		roomIDs = append(roomIDs, rid)
		unique[rid] = struct{}{}
	}

	t.Logf("Unique rooms created: %d", len(unique))
	for r := range unique {
		t.Logf("  room: %s", r)
	}

	// Print server-side room assignment for each session
	for i := 0; i < 8; i++ {
		s := waitSession(t, sessions, fmt.Sprintf("missing session %d", i+1))
		if s.Room() != nil {
			t.Logf("Server session %d is in room: %s", i+1, s.Room().Name())
		} else {
			t.Logf("Server session %d has no room", i+1)
		}
	}
}

// waitQueueEventProtobuf reads messages until a protobuf QueueStatus is received,
// skipping any JSON text messages that may be sent alongside protobuf.
func waitQueueEventProtobuf(t *testing.T, c *wsClient) *matchmaking.QueueStatus {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			t.Fatal("timed out waiting for protobuf queue event")
		}

		op, payload, err := c.readServerMessage(timeout)
		if err != nil {
			t.Fatalf("read server message: %v", err)
		}
		if op != gws.OpcodeBinary {
			continue
		}

		event := &matchmaking.QueueStatus{}
		if err := proto.Unmarshal(payload, event); err != nil {
			t.Fatalf("unmarshal protobuf event: %v", err)
		}

		return event
	}
}

// TestMatchmakerWithProtobuf verifies that matchmaker sends QueueStatus messages
// as protobuf binary when the protocol is set to Protobuf.
func TestMatchmakerWithProtobuf(t *testing.T) {
	a := adler.New(
		adler.WithDispatchAsync(true),
		adler.WithMessageBufferSize(8),
		adler.WithProtocol(adler.Protobuf),
	)
	mm := matchmaking.NewMatchmaker(a, matchmaking.WithRoomSize(2))
	sessions := make(chan *adler.Session, 8)

	a.HandleConnect(func(s *adler.Session) {
		mm.AddToQueue(s)
		sessions <- s
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = a.HandleRequest(w, r)
	}))
	defer srv.Close()

	// Connect two clients
	conn1 := dialWS(t, wsURL(srv.URL)+"?cid=1")
	defer conn1.Close()

	conn2 := dialWS(t, wsURL(srv.URL)+"?cid=2")
	defer conn2.Close()

	_ = waitSession(t, sessions, "missing first session")
	_ = waitSession(t, sessions, "missing second session")

	// First client should receive queue_joined
	evt1 := waitQueueEventProtobuf(t, conn1)
	if evt1.GetQueueJoined() == nil {
		t.Fatalf("expected QueueJoined, got: %T", evt1.Payload)
	}
	if evt1.GetQueueJoined().Message != "You have joined the main queue" {
		t.Fatalf("unexpected message: %q", evt1.GetQueueJoined().Message)
	}

	// Second client should receive queue_joined
	evt2 := waitQueueEventProtobuf(t, conn2)
	if evt2.GetQueueJoined() == nil {
		t.Fatalf("expected QueueJoined, got: %T", evt2.Payload)
	}

	// Both should receive match_found (order may vary)
	match1 := waitQueueEventProtobuf(t, conn1)
	match2 := waitQueueEventProtobuf(t, conn2)

	if match1.GetMatchFound() == nil {
		t.Fatalf("expected MatchFound for conn1, got: %T", match1.Payload)
	}
	if match2.GetMatchFound() == nil {
		t.Fatalf("expected MatchFound for conn2, got: %T", match2.Payload)
	}

	// Both should receive the same room ID
	if match1.GetMatchFound().RoomId != match2.GetMatchFound().RoomId {
		t.Fatalf("room IDs don't match: %q vs %q", match1.GetMatchFound().RoomId, match2.GetMatchFound().RoomId)
	}

	if match1.GetMatchFound().Players != 2 {
		t.Fatalf("expected 2 players, got %d", match1.GetMatchFound().Players)
	}
}
