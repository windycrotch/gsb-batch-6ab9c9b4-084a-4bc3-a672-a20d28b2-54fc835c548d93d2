package rtmp

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/im-pingo/liveforge/core"
	"github.com/im-pingo/liveforge/pkg/avframe"
)

// rtmpLifecycleEvents records publish start/stop contexts per publisher so
// tests can assert generation-scoped pairing.
type rtmpLifecycleEvents struct {
	mu     sync.Mutex
	starts []*core.EventContext
	stops  []*core.EventContext
}

func newLifecycleRecorder(bus *core.EventBus) *rtmpLifecycleEvents {
	rec := &rtmpLifecycleEvents{}
	bus.Register(core.HookRegistration{Event: core.EventPublish, Mode: core.HookAsync, Consumer: "takeover-recorder", Handler: func(ctx *core.EventContext) error {
		rec.mu.Lock()
		rec.starts = append(rec.starts, ctx)
		rec.mu.Unlock()
		return nil
	}})
	bus.Register(core.HookRegistration{Event: core.EventPublishStop, Mode: core.HookAsync, Consumer: "takeover-recorder", Handler: func(ctx *core.EventContext) error {
		rec.mu.Lock()
		rec.stops = append(rec.stops, ctx)
		rec.mu.Unlock()
		return nil
	}})
	return rec
}

func (r *rtmpLifecycleEvents) snapshot() (starts, stops []*core.EventContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*core.EventContext(nil), r.starts...), append([]*core.EventContext(nil), r.stops...)
}

func (r *rtmpLifecycleEvents) waitForCount(wantStarts, wantStops int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		starts, stops := r.snapshot()
		if len(starts) >= wantStarts && len(stops) >= wantStops {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// newTakeoverHandler wires a Handler to one end of an in-memory pipe; the
// returned client connection is drained continuously so server writes never
// block the handler goroutine.
func newTakeoverHandler(t *testing.T, hub *core.StreamHub, bus *core.EventBus) (*Handler, net.Conn, net.Conn) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	go func() { _, _ = io.Copy(io.Discard, clientConn) }()
	handler := NewHandler(serverConn, hub, bus, 4096, nil)
	handler.app = "live"
	return handler, clientConn, serverConn
}

func publishTakeover(handler *Handler, name string) error {
	return handler.onPublish([]any{"publish", float64(0), nil, name})
}

func takeoverVideoSeqHeader(codecByte byte) []byte {
	return []byte{0x17, 0x00, 0x00, 0x00, 0x00, 0x01, codecByte, 0x00, 0x1f}
}

func takeoverVideoKeyframe(payload byte) []byte {
	return []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0x65, payload}
}

func TestRTMPPublishTakeoverTransfersGenerationOwnership(t *testing.T) {
	hub, bus := newTestHub()
	rec := newLifecycleRecorder(bus)
	streamKey := "live/takeover"

	oldHandler, _, _ := newTakeoverHandler(t, hub, bus)
	if err := publishTakeover(oldHandler, "takeover"); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	stream, ok := hub.Find(streamKey)
	if !ok {
		t.Fatal("stream not found after first publish")
	}
	oldPublisher := oldHandler.publisher
	oldGeneration := oldHandler.publisherGeneration
	if oldGeneration == 0 || stream.Publisher() != oldPublisher {
		t.Fatal("first publisher did not own the stream")
	}

	// The first generation accepts frames and owns its codec state.
	oldHeader := takeoverVideoSeqHeader(0x64)
	if err := oldHandler.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: oldHeader}); err != nil {
		t.Fatalf("first publisher header: %v", err)
	}
	if oldPublisher.MediaInfo().VideoCodec != avframe.CodecH264 {
		t.Fatal("first publisher media info was not updated")
	}
	cursorAfterOld := stream.RingBuffer().WriteCursor()

	newHandler, _, _ := newTakeoverHandler(t, hub, bus)
	if err := publishTakeover(newHandler, "takeover"); err != nil {
		t.Fatalf("takeover publish: %v", err)
	}
	newPublisher := newHandler.publisher
	if newPublisher == oldPublisher {
		t.Fatal("takeover reused the old publisher object")
	}
	if stream.Publisher() != newPublisher {
		t.Fatal("takeover did not install the new publisher")
	}
	if newHandler.publisherGeneration != oldGeneration+1 {
		t.Fatalf("new generation = %d, want %d", newHandler.publisherGeneration, oldGeneration+1)
	}
	if newHandler.streamInstanceID != oldHandler.streamInstanceID {
		t.Fatal("takeover changed the stream instance")
	}
	snapshot := stream.StartupSnapshot()
	if snapshot.GenerationStartCursor != cursorAfterOld {
		t.Fatalf("new generation start cursor = %d, want %d", snapshot.GenerationStartCursor, cursorAfterOld)
	}
	if newPublisher.MediaInfo().VideoCodec != 0 {
		t.Fatal("new publisher inherited codec state from the old generation")
	}

	// Old publisher's subsequent media must be rejected, must not move the new
	// generation's write cursor, and must not rewrite either publisher's info.
	staleHeader := takeoverVideoSeqHeader(0x42)
	if err := oldHandler.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: staleHeader}); err == nil {
		t.Fatal("stale publisher header was not rejected")
	}
	staleKey := takeoverVideoKeyframe(0x99)
	if err := oldHandler.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: staleKey}); err == nil {
		t.Fatal("stale publisher keyframe was not rejected")
	}
	if got := stream.RingBuffer().WriteCursor(); got != cursorAfterOld {
		t.Fatalf("stale publisher advanced cursor from %d to %d", cursorAfterOld, got)
	}
	if got := newPublisher.MediaInfo().VideoCodec; got != 0 {
		t.Fatalf("stale publisher changed replacement codec to %v", got)
	}
	if got := oldPublisher.MediaInfo().VideoSequenceHeader; len(got) != len(oldHeader)-5 {
		// The stale header must not mutate even the detached publisher object.
		t.Fatalf("stale publisher rewrote its own media info: %x", got)
	}

	// New publisher writes land in the fresh generation and update its info.
	if err := newHandler.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: takeoverVideoSeqHeader(0x42)}); err != nil {
		t.Fatalf("replacement header: %v", err)
	}
	if err := newHandler.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: takeoverVideoKeyframe(0x01)}); err != nil {
		t.Fatalf("replacement keyframe: %v", err)
	}
	if newPublisher.MediaInfo().VideoCodec != avframe.CodecH264 {
		t.Fatal("replacement publisher media info not updated by its own header")
	}
	if got := stream.RingBuffer().WriteCursor(); got != cursorAfterOld+2 {
		t.Fatalf("replacement cursor = %d, want %d", got, cursorAfterOld+2)
	}
	if !stream.IsPublisherGeneration(newHandler.publisherGeneration) {
		t.Fatal("replacement generation is not active")
	}
	if stream.IsPublisherGeneration(oldGeneration) {
		t.Fatal("old generation still reports active after takeover")
	}

	// Old connection cleanup must not detach the replacement and must report
	// exactly one stop for the old generation.
	oldHandler.cleanup()
	oldHandler.cleanup() // duplicate cleanup is a no-op
	if stream.Publisher() != newPublisher || stream.State() != core.StreamStatePublishing {
		t.Fatal("old connection cleanup removed the replacement publisher")
	}
	if !rec.waitForCount(2, 1) {
		starts, stops := rec.snapshot()
		t.Fatalf("lifecycle events not paired: starts=%d stops=%d", len(starts), len(stops))
	}
	starts, stops := rec.snapshot()
	if len(starts) != 2 {
		t.Fatalf("start events = %d, want 2", len(starts))
	}
	if len(stops) != 1 {
		t.Fatalf("stop events = %d, want 1 (old generation only)", len(stops))
	}
	stop := stops[0]
	if stop.PublisherGeneration != oldGeneration || stop.PublisherID != oldPublisher.ID() ||
		stop.StreamInstanceID != oldHandler.streamInstanceID {
		t.Fatalf("old stop identity = %+v, want generation %d publisher %s", stop, oldGeneration, oldPublisher.ID())
	}
	for _, start := range starts {
		if start.PublisherID == "" || start.PublisherGeneration == 0 || start.StreamInstanceID == 0 {
			t.Fatalf("incomplete start event identity: %+v", start)
		}
	}

	// Closing the replacement then reports its own paired stop, once.
	newHandler.cleanup()
	newHandler.cleanup()
	if !rec.waitForCount(2, 2) {
		_, stops = rec.snapshot()
		t.Fatalf("stop events = %d, want 2", len(stops))
	}
	_, stops = rec.snapshot()
	if stops[1].PublisherGeneration != newHandler.publisherGeneration || stops[1].PublisherID != newPublisher.ID() {
		t.Fatalf("new stop identity = %+v, want generation %d publisher %s", stops[1], newHandler.publisherGeneration, newPublisher.ID())
	}
}

func TestRTMPTakeoverClosesDisplacedConnection(t *testing.T) {
	hub, bus := newTestHub()
	oldHandler, oldClient, _ := newTakeoverHandler(t, hub, bus)
	if err := publishTakeover(oldHandler, "kick-conn"); err != nil {
		t.Fatal(err)
	}
	newHandler, _, _ := newTakeoverHandler(t, hub, bus)
	if err := publishTakeover(newHandler, "kick-conn"); err != nil {
		t.Fatalf("takeover publish: %v", err)
	}

	// The displaced publisher's TCP connection must be closed so its blocking
	// read loop terminates instead of lingering as a ghost writer.
	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		_, err := oldClient.Read(buf)
		readErr <- err
	}()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("displaced connection read succeeded after takeover")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("displaced connection was not closed by takeover")
	}

	// Handle() on the old connection must terminate (its read loop unblocks)
	// rather than lingering as a ghost writer after the takeover close.
	done := make(chan error, 1)
	go func() { done <- oldHandler.Handle() }()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) &&
			!isConnClosed(err) && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("displaced handler returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("displaced handler did not stop after takeover")
	}
}

func TestRTMPRejectedPublishLeavesNoStopEvent(t *testing.T) {
	hub, bus := newTestHub()
	rec := newLifecycleRecorder(bus)

	// A synchronous authorization rejection is the real admission gate: the
	// handler must return before installing a publisher and before any async
	// lifecycle event is admitted, so no terminal event can ever pair with it.
	authErr := errors.New("publish denied by test authorizer")
	bus.Register(core.HookRegistration{
		Event:    core.EventPublish,
		Mode:     core.HookSync,
		Consumer: "takeover-reject-auth",
		Handler:  func(*core.EventContext) error { return authErr },
	})

	streamKey := "live/rejected"
	handler, _, _ := newTakeoverHandler(t, hub, bus)
	err := publishTakeover(handler, "rejected")
	if !errors.Is(err, authErr) {
		t.Fatalf("publish error = %v, want authorization rejection", err)
	}
	if handler.isPublisher || handler.publisher != nil {
		t.Fatal("rejected handler retained publisher state")
	}
	handler.cleanup()
	// Give any (incorrect) terminal event time to surface before asserting.
	time.Sleep(50 * time.Millisecond)
	starts, stops := rec.snapshot()
	if len(starts) != 0 || len(stops) != 0 {
		t.Fatalf("rejected publish produced lifecycle events: starts=%d stops=%d", len(starts), len(stops))
	}
	if stream, ok := hub.Find(streamKey); ok && stream.Publisher() != nil {
		t.Fatal("rejected publish installed a publisher")
	}
}

func TestRTMPDisconnectAndRepublishGetIndependentGenerations(t *testing.T) {
	hub, bus := newTestHub()
	rec := newLifecycleRecorder(bus)
	streamKey := "live/reconnect"

	first, _, _ := newTakeoverHandler(t, hub, bus)
	if err := publishTakeover(first, "reconnect"); err != nil {
		t.Fatal(err)
	}
	firstGeneration := first.publisherGeneration
	firstPublisher := first.publisher
	if err := first.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: takeoverVideoSeqHeader(0x64)}); err != nil {
		t.Fatal(err)
	}
	if err := first.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: takeoverVideoKeyframe(0x01)}); err != nil {
		t.Fatal(err)
	}
	cursorBeforeClose := hub2Cursor(t, hub, streamKey)
	first.cleanup()
	if !rec.waitForCount(1, 1) {
		t.Fatal("first generation stop event missing")
	}
	starts, stops := rec.snapshot()
	if stops[0].PublisherGeneration != firstGeneration || stops[0].PublisherID != firstPublisher.ID() {
		t.Fatalf("first stop = %+v", stops[0])
	}

	second, _, _ := newTakeoverHandler(t, hub, bus)
	if err := publishTakeover(second, "reconnect"); err != nil {
		t.Fatal(err)
	}
	if second.publisherGeneration != firstGeneration+1 {
		t.Fatalf("reconnect generation = %d, want %d", second.publisherGeneration, firstGeneration+1)
	}
	if second.streamInstanceID != first.streamInstanceID {
		t.Fatal("reconnect created a new stream instance")
	}
	snapshot, ok := hub.Find(streamKey)
	if !ok {
		t.Fatal("stream missing after reconnect")
	}
	startup := snapshot.StartupSnapshot()
	if startup.GenerationStartCursor != cursorBeforeClose {
		t.Fatalf("reconnect start cursor = %d, want %d", startup.GenerationStartCursor, cursorBeforeClose)
	}
	if second.publisher.MediaInfo().VideoCodec != 0 {
		t.Fatal("reconnect inherited codec info from the previous session")
	}
	// Viewers of the new generation start from the new generation origin:
	// replay frames from the previous generation are not carried over.
	if len(startup.ReplayFrames) != 0 {
		t.Fatalf("reconnect generation retained %d replay frames", len(startup.ReplayFrames))
	}
	if err := second.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: takeoverVideoSeqHeader(0x42)}); err != nil {
		t.Fatal(err)
	}
	if err := second.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: takeoverVideoKeyframe(0x02)}); err != nil {
		t.Fatal(err)
	}
	if got := snapshot.RingBuffer().WriteCursor(); got != cursorBeforeClose+2 {
		t.Fatalf("reconnect cursor = %d, want %d", got, cursorBeforeClose+2)
	}
	second.cleanup()
	if !rec.waitForCount(2, 2) {
		starts, stops = rec.snapshot()
		t.Fatalf("reconnect events: starts=%d stops=%d", len(starts), len(stops))
	}
	starts, stops = rec.snapshot()
	for i, stop := range stops {
		if stop.PublisherID != starts[i].PublisherID || stop.PublisherGeneration != starts[i].PublisherGeneration {
			t.Fatalf("stop %d not paired with its own start: stop=%+v start=%+v", i, stop, starts[i])
		}
	}
}

func TestRTMPSinglePublisherLifecycleBehaviorUnchanged(t *testing.T) {
	hub, bus := newTestHub()
	rec := newLifecycleRecorder(bus)
	streamKey := "live/single"

	handler, _, _ := newTakeoverHandler(t, hub, bus)
	if err := publishTakeover(handler, "single"); err != nil {
		t.Fatal(err)
	}
	stream, ok := hub.Find(streamKey)
	if !ok || stream.State() != core.StreamStatePublishing {
		t.Fatalf("single publish state = %v", stream)
	}
	pub := handler.publisher
	if err := handler.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: takeoverVideoSeqHeader(0x64)}); err != nil {
		t.Fatal(err)
	}
	if err := handler.handleMediaMessage(&Message{TypeID: MsgVideo, Payload: takeoverVideoKeyframe(0x07)}); err != nil {
		t.Fatal(err)
	}
	if pub.MediaInfo().VideoCodec != avframe.CodecH264 {
		t.Fatal("single publisher media info not updated")
	}
	if got := stream.RingBuffer().WriteCursor(); got != 2 {
		t.Fatalf("single publisher cursor = %d, want 2", got)
	}
	if gop := stream.GOPCache(); len(gop) != 1 {
		t.Fatalf("single publisher GOP cache = %d frames, want 1", len(gop))
	}

	// Normal disconnect: one paired stop, publisher detached, no-publisher state.
	handler.cleanup()
	if !rec.waitForCount(1, 1) {
		t.Fatal("single publisher lifecycle event missing")
	}
	starts, stops := rec.snapshot()
	if len(starts) != 1 || len(stops) != 1 {
		t.Fatalf("single publisher events: starts=%d stops=%d", len(starts), len(stops))
	}
	if stops[0].PublisherID != starts[0].PublisherID || stops[0].PublisherGeneration != starts[0].PublisherGeneration {
		t.Fatal("single publisher stop not paired with its start")
	}
	if stream.Publisher() != nil || stream.State() != core.StreamStateNoPublisher {
		t.Fatalf("single publisher state after cleanup: %s publisher=%v", stream.State(), stream.Publisher())
	}
}

func hub2Cursor(t *testing.T, hub *core.StreamHub, key string) int64 {
	t.Helper()
	stream, ok := hub.Find(key)
	if !ok {
		t.Fatal("stream not found")
	}
	return stream.RingBuffer().WriteCursor()
}
