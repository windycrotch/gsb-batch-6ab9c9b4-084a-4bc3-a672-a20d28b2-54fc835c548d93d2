package rtmp

import (
	"net"
	"testing"
	"time"

	"github.com/im-pingo/liveforge/core"
)

// wireRTMPClient is a minimal RTMP client over an in-memory pipe that performs
// the handshake and command/media exchange against a real Handler.Handle loop.
type wireRTMPClient struct {
	t    *testing.T
	conn net.Conn
	cw   *ChunkWriter
	cr   *ChunkReader
}

func newWireRTMPClient(t *testing.T, hub *core.StreamHub, bus *core.EventBus) *wireRTMPClient {
	t.Helper()
	clientConn, peer := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })

	// Mirror the real server entry point (server.go): the handshake completes
	// first, and only then does the handler's read loop own the connection.
	handshakeErr := make(chan error, 1)
	go func() { handshakeErr <- clientHandshake(clientConn) }()
	if err := ServerHandshake(peer); err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	if err := <-handshakeErr; err != nil {
		t.Fatalf("client handshake: %v", err)
	}

	handler := NewHandler(peer, hub, bus, 4096, nil)
	handleDone := make(chan error, 1)
	go func() { handleDone <- handler.Handle() }()
	t.Cleanup(func() {
		_ = peer.Close()
		select {
		case <-handleDone:
		case <-time.After(2 * time.Second):
		}
	})

	c := &wireRTMPClient{
		t:    t,
		conn: clientConn,
		cw:   NewChunkWriter(clientConn, DefaultChunkSize),
		cr:   NewChunkReader(clientConn, DefaultChunkSize),
	}
	c.connect()
	return c
}

func (c *wireRTMPClient) connect() {
	payload, _ := AMF0Encode("connect", float64(1), map[string]any{"app": "live"})
	if err := c.cw.WriteMessage(3, &Message{TypeID: MsgAMF0Command, Length: uint32(len(payload)), Payload: payload}); err != nil {
		c.t.Fatalf("write connect: %v", err)
	}
	// WindowAckSize, PeerBandwidth, SetChunkSize, _result
	for range 4 {
		msg, err := c.cr.ReadMessage()
		if err != nil {
			c.t.Fatalf("read connect response: %v", err)
		}
		if msg.TypeID == MsgSetChunkSize && len(msg.Payload) >= 4 {
			c.cr.SetChunkSize(int(uint32(msg.Payload[0])<<24 | uint32(msg.Payload[1])<<16 | uint32(msg.Payload[2])<<8 | uint32(msg.Payload[3])))
		}
	}
}

func (c *wireRTMPClient) createStream() {
	payload, _ := AMF0Encode("createStream", float64(2), nil)
	if err := c.cw.WriteMessage(3, &Message{TypeID: MsgAMF0Command, Length: uint32(len(payload)), Payload: payload}); err != nil {
		c.t.Fatalf("write createStream: %v", err)
	}
	if _, err := c.cr.ReadMessage(); err != nil {
		c.t.Fatalf("read createStream response: %v", err)
	}
}

// publish runs the full publish command exchange and returns when the server
// accepts (or rejects) it.
func (c *wireRTMPClient) publish(name string) {
	c.createStream()
	payload, _ := AMF0Encode("publish", float64(0), nil, name, "live")
	if err := c.cw.WriteMessage(3, &Message{TypeID: MsgAMF0Command, Length: uint32(len(payload)), Payload: payload}); err != nil {
		c.t.Fatalf("write publish: %v", err)
	}
	msg, err := c.cr.ReadMessage()
	if err != nil {
		c.t.Fatalf("read publish onStatus: %v", err)
	}
	vals, err := AMF0Decode(msg.Payload)
	if err != nil || len(vals) == 0 || vals[0] != "onStatus" {
		c.t.Fatalf("expected onStatus, got %v err=%v", vals, err)
	}
}

func (c *wireRTMPClient) writeVideo(payload []byte, timestamp uint32) {
	if err := c.cw.WriteMessage(6, &Message{
		TypeID: MsgVideo, Length: uint32(len(payload)), Timestamp: timestamp, StreamID: 1, Payload: payload,
	}); err != nil {
		c.t.Fatalf("write video: %v", err)
	}
}

// writeVideoOrClosed sends a media frame but tolerates the connection already
// being closed, which is the expected outcome for a displaced publisher.
func (c *wireRTMPClient) writeVideoOrClosed(payload []byte, timestamp uint32) error {
	return c.cw.WriteMessage(6, &Message{
		TypeID: MsgVideo, Length: uint32(len(payload)), Timestamp: timestamp, StreamID: 1, Payload: payload,
	})
}

// TestRTMPWireTakeoverRejectsStaleFramesAndPairsLifecycle drives the actual
// RTMP server entry point (handshake + chunked command/media loop) for the
// replacement timing described in the task: a second publisher takes over,
// the displaced connection keeps sending media and gets terminated, and its
// later cleanup cannot detach the new generation.
func TestRTMPWireTakeoverRejectsStaleFramesAndPairsLifecycle(t *testing.T) {
	hub, bus := newTestHub()
	rec := newLifecycleRecorder(bus)
	streamKey := "live/wire-takeover"

	oldClient := newWireRTMPClient(t, hub, bus)
	oldClient.publish("wire-takeover")
	stream, ok := hub.Find(streamKey)
	if !ok {
		t.Fatal("stream missing after first wire publish")
	}
	oldGeneration := stream.StartupSnapshot().Generation
	oldClient.writeVideo(takeoverVideoSeqHeader(0x64), 0)
	oldClient.writeVideo(takeoverVideoKeyframe(0x01), 0)
	waitForCursor(t, stream, 2)

	newClient := newWireRTMPClient(t, hub, bus)
	newClient.publish("wire-takeover")
	newGeneration := stream.StartupSnapshot().Generation
	if newGeneration != oldGeneration+1 {
		t.Fatalf("wire takeover generation = %d, want %d", newGeneration, oldGeneration+1)
	}
	if stream.State() != core.StreamStatePublishing {
		t.Fatalf("stream state after takeover = %s", stream.State())
	}
	cursorAtTakeover := stream.RingBuffer().WriteCursor()

	// The displaced connection is closed by the takeover, so its read loop is
	// gone before it can inject any more media. The client must observe that
	// closure rather than keep a ghost publishing channel open.
	readErr := make(chan error, 1)
	go func() {
		_, err := oldClient.cr.ReadMessage()
		readErr <- err
	}()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("displaced wire connection stayed open after takeover")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("displaced wire connection was not terminated by takeover")
	}

	// Frames the displaced client keeps trying to push cannot reach the ring:
	// writes now fail on the closed connection and never touch the new
	// generation's write cursor or codec state.
	if err := oldClient.writeVideoOrClosed(takeoverVideoSeqHeader(0x42), 100); err == nil {
		t.Fatal("displaced client write succeeded after its connection closed")
	}
	if err := oldClient.writeVideoOrClosed(takeoverVideoKeyframe(0x99), 100); err == nil {
		t.Fatal("displaced client second write succeeded after its connection closed")
	}
	if got := stream.RingBuffer().WriteCursor(); got != cursorAtTakeover {
		t.Fatalf("stale wire writes advanced cursor from %d to %d", cursorAtTakeover, got)
	}
	if got := stream.StartupSnapshot().Generation; got != newGeneration {
		t.Fatalf("generation changed to %d after stale writes, want %d", got, newGeneration)
	}

	// The new publisher's keyframe lands in the new generation origin.
	newClient.writeVideo(takeoverVideoSeqHeader(0x42), 0)
	newClient.writeVideo(takeoverVideoKeyframe(0x02), 0)
	waitForCursor(t, stream, cursorAtTakeover+2)
	if !stream.IsPublisherGeneration(newGeneration) {
		t.Fatal("new generation not active after stale connection termination")
	}

	// Lifecycle: two starts, and only the old generation's stop so far; closing
	// the new connection adds its own exactly-once stop.
	if !rec.waitForCount(2, 1) {
		starts, stops := rec.snapshot()
		t.Fatalf("wire lifecycle events: starts=%d stops=%d", len(starts), len(stops))
	}
	newClient.conn.Close()
	if !rec.waitForCount(2, 2) {
		_, stops := rec.snapshot()
		t.Fatalf("wire stop events = %d, want 2", len(stops))
	}
	starts, stops := rec.snapshot()
	for i, stop := range stops {
		if stop.PublisherID != starts[i].PublisherID || stop.PublisherGeneration != starts[i].PublisherGeneration {
			t.Fatalf("wire stop %d not paired with its start: stop=%+v start=%+v", i, stop, starts[i])
		}
	}
}

func waitForCursor(t *testing.T, stream *core.Stream, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stream.RingBuffer().WriteCursor() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("write cursor = %d, want %d", stream.RingBuffer().WriteCursor(), want)
}

// TestRTMPWireSinglePublisherUnaffected confirms the ordinary path through
// the real connection entry point keeps working after the takeover changes.
func TestRTMPWireSinglePublisherUnaffected(t *testing.T) {
	hub, bus := newTestHub()
	rec := newLifecycleRecorder(bus)

	client := newWireRTMPClient(t, hub, bus)
	client.publish("wire-single")
	stream, ok := hub.Find("live/wire-single")
	if !ok || stream.State() != core.StreamStatePublishing {
		t.Fatalf("single wire publish state = %v", stream)
	}
	client.writeVideo(takeoverVideoSeqHeader(0x64), 0)
	client.writeVideo(takeoverVideoKeyframe(0x07), 0)
	waitForCursor(t, stream, 2)

	client.conn.Close()
	if !rec.waitForCount(1, 1) {
		starts, stops := rec.snapshot()
		t.Fatalf("single wire lifecycle: starts=%d stops=%d", len(starts), len(stops))
	}
	// The server-side handler should settle into no_publisher once it notices
	// the disconnect.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stream.State() == core.StreamStateNoPublisher {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("stream state after disconnect = %s, want no_publisher", stream.State())
}
