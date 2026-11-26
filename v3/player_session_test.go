package vlc

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file is the locally runnable verification suite for the player/media
// session lifecycle hardening.
//
// It deliberately does NOT depend on any real audio or video file:
//
//   - Session generation, stale callback suppression, re-entrant Stop/Release
//     and idempotent Detach are verified deterministically by injecting events
//     straight into the package eventDispatch entry point. The events pass
//     through the real eventRegistry, eventGate and Player wiring; only the
//     underlying native event source is simulated.
//   - Reader lifetime / Duplicate sharing is verified through the real
//     mediaBufferOpenCB / ReadCB / SeekCB production callbacks using in-memory
//     io.ReadSeeker spies.
//   - One end-to-end test plays a tiny WAV generated in memory. No media file
//     is read from disk.
//
// Running:
//
//	go test ./v3
//
// On a machine with libVLC installed normally, the cgo flags are detected
// automatically. On a custom Windows toolchain/SDK location, provide the
// headers/import library and runtime DLL directory, for example in Git Bash:
//
//	PATH="/c/path/to/vlc:$PATH" \
//	CGO_CFLAGS="-I/c/path/to/vlc/sdk/include" \
//	CGO_LDFLAGS="-L/c/path/to/import-lib-dir" \
//	go test -race ./v3
//
// If a usable libVLC runtime cannot be initialised, the native tests skip while
// the pure session-gate tests still run.

// psInitOnce initialises libVLC exactly once for the whole test binary. It is
// idempotent with any TestMain another test file may provide, because Init
// itself is a no-op once the global instance exists.
var psInitOnce sync.Once
var psInitErr error

func psEnsureVLC(t *testing.T) {
	t.Helper()
	psInitOnce.Do(func() {
		psInitErr = Init("--no-video", "--quiet", "--aout=dummy")
	})
	if psInitErr != nil {
		t.Skipf("libVLC runtime unavailable, skipping native test: %v", psInitErr)
	}
}

func psNewPlayer(t *testing.T) *Player {
	t.Helper()
	psEnsureVLC(t)
	p, err := NewPlayer()
	if err != nil {
		t.Fatalf("NewPlayer: %v", err)
	}
	return p
}

// psRelease releases the player and waits for native teardown to finish, so
// re-entrant (asynchronous) release paths cannot outlive the test.
func psRelease(t *testing.T, p *Player) {
	t.Helper()
	if p == nil {
		return
	}
	if err := p.Release(); err != nil {
		t.Fatalf("Player.Release: %v", err)
	}

	p.mu.Lock()
	done := p.releaseDone
	p.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for native player release")
	}
}

func psMediaBytes(t *testing.T, data []byte) *Media {
	t.Helper()
	psEnsureVLC(t)
	m, err := NewMediaFromReadSeeker(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewMediaFromReadSeeker: %v", err)
	}
	return m
}

// psDispatch simulates libVLC invoking the event trampoline for eventID. It
// exercises the exact registry -> gate -> user callback dispatch path
// deterministically, without depending on media timing or synthesising C
// pointers.
func psDispatch(eventID EventID) {
	dispatchEventByID(eventID, nil)
}

func psAttachCounter(t *testing.T, em *EventManager, ev Event) (EventID, *int32) {
	t.Helper()
	var calls int32
	id, err := em.Attach(ev, func(got Event, userData interface{}) {
		atomic.AddInt32(&calls, 1)
	}, nil)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	return id, &calls
}

// ---------------------------------------------------------------------------
// Pure session gate tests (no libVLC runtime required).
// ---------------------------------------------------------------------------

func TestPSEventGateEpochSuppression(t *testing.T) {
	g := newEventGate()

	// A subscription created before any media is set uses the initial epoch
	// and behaves as a player-lifetime subscription.
	lifetimeEpoch := g.stamp()
	if lifetimeEpoch != initialSessionEpoch {
		t.Fatalf("initial epoch = %d, want %d", lifetimeEpoch, initialSessionEpoch)
	}

	first := g.rotate()
	if first == initialSessionEpoch {
		t.Fatal("first media session must open a non-initial epoch")
	}
	firstSub := g.stamp()
	if firstSub != first {
		t.Fatalf("stamp = %d, want current epoch %d", firstSub, first)
	}

	// Lifetime subscription survives rotation.
	done, ok := g.enter(lifetimeEpoch)
	if !ok {
		t.Fatal("lifetime subscription must be accepted after rotation")
	}
	done()

	// Current session subscription is accepted.
	done, ok = g.enter(firstSub)
	if !ok {
		t.Fatal("current session subscription must be accepted")
	}
	done()

	// Switching media rotates the epoch: the first session subscription is
	// now stale and must be rejected, while a new subscription is accepted.
	second := g.rotate()
	if second == first {
		t.Fatal("epoch must advance on media switch")
	}
	if _, ok := g.enter(firstSub); ok {
		t.Fatal("stale first-session event must be rejected after switch")
	}

	secondSub := g.stamp()
	done, ok = g.enter(secondSub)
	if !ok {
		t.Fatal("second session subscription must be accepted")
	}
	done()

	// Closing the gate rejects everything, including lifetime subscriptions.
	g.close()
	if _, ok := g.enter(secondSub); ok {
		t.Fatal("dispatch after close must be rejected")
	}
	if _, ok := g.enter(lifetimeEpoch); ok {
		t.Fatal("lifetime dispatch after close must be rejected")
	}
}

func TestPSEventGateWaitDrainsCallbacks(t *testing.T) {
	g := newEventGate()
	epoch := g.stamp()

	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})

	go func() {
		done, ok := g.enter(epoch)
		if !ok {
			t.Error("expected dispatch to be accepted")
			return
		}
		close(entered)
		<-release
		done()
		close(finished)
	}()

	<-entered

	waited := make(chan struct{})
	go func() {
		g.close()
		g.wait()
		close(waited)
	}()

	select {
	case <-waited:
		t.Fatal("wait returned before callback finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("callback did not finish")
	}
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("wait did not unblock after callback finished")
	}
}

// ---------------------------------------------------------------------------
// A1: stale media callbacks are suppressed after switching media.
// ---------------------------------------------------------------------------

func TestPSStaleMediaCallbacksSuppressed(t *testing.T) {
	p := psNewPlayer(t)
	defer psRelease(t, p)

	mediaA := psMediaBytes(t, bytes.Repeat([]byte("A"), 4096))
	if err := p.SetMedia(mediaA); err != nil {
		t.Fatalf("SetMedia(A): %v", err)
	}
	defer mediaA.Release()

	em, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}

	idA, callsA := psAttachCounter(t, em, MediaPlayerTimeChanged)

	// Sanity: a current-session event is delivered.
	psDispatch(idA)
	if got := atomic.LoadInt32(callsA); got != 1 {
		t.Fatalf("A callback count before switch = %d, want 1", got)
	}

	// Switch to media B. This rotates the session epoch.
	mediaB := psMediaBytes(t, bytes.Repeat([]byte("B"), 4096))
	if err := p.SetMedia(mediaB); err != nil {
		t.Fatalf("SetMedia(B): %v", err)
	}
	defer mediaB.Release()

	// Late events belonging to A must not reach user code, even repeatedly.
	for i := 0; i < 5; i++ {
		psDispatch(idA)
	}
	if got := atomic.LoadInt32(callsA); got != 1 {
		t.Fatalf("A callback count after switch = %d, want unchanged 1", got)
	}

	// A callback attached for session B is delivered.
	idB, callsB := psAttachCounter(t, em, MediaPlayerTimeChanged)
	psDispatch(idB)
	psDispatch(idA) // still stale
	if got := atomic.LoadInt32(callsB); got != 1 {
		t.Fatalf("B callback count = %d, want 1", got)
	}
	if got := atomic.LoadInt32(callsA); got != 1 {
		t.Fatalf("A callback count increased to %d after B events", got)
	}

	// An EventManager obtained later shares the same player gate.
	em2, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}
	idC, callsC := psAttachCounter(t, em2, MediaPlayerTimeChanged)
	psDispatch(idC)
	if got := atomic.LoadInt32(callsC); got != 1 {
		t.Fatalf("C callback count = %d, want 1", got)
	}

	em.Detach(idA, idB)
	em2.Detach(idC)
}

// Attachments made before the first media is set are player-lifetime
// subscriptions and must keep working across switches (backwards compatible).
func TestPSLifetimeAttachmentSurvivesSwitch(t *testing.T) {
	p := psNewPlayer(t)
	defer psRelease(t, p)

	em, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}
	id, calls := psAttachCounter(t, em, MediaPlayerTimeChanged)

	m1 := psMediaBytes(t, []byte("one"))
	if err := p.SetMedia(m1); err != nil {
		t.Fatal(err)
	}
	m2 := psMediaBytes(t, []byte("two"))
	if err := p.SetMedia(m2); err != nil {
		t.Fatal(err)
	}

	psDispatch(id)
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Fatalf("lifetime callback count = %d, want 1", got)
	}
	em.Detach(id)
	m1.Release()
	m2.Release()
}

// ---------------------------------------------------------------------------
// A2: Stop / Release from inside a callback.
// ---------------------------------------------------------------------------

func TestPSStopInsideCallback(t *testing.T) {
	p := psNewPlayer(t)
	defer psRelease(t, p)

	m := psMediaBytes(t, psMakeWAV(120))
	if err := p.SetMedia(m); err != nil {
		t.Fatal(err)
	}
	defer m.Release()

	em, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}

	var stops int32
	var stopErr atomic.Value // error
	id, err := em.Attach(MediaPlayerTimeChanged, func(ev Event, ud interface{}) {
		atomic.AddInt32(&stops, 1)
		if err := p.Stop(); err != nil {
			stopErr.Store(err)
		}
		// Repeated stop from within the same callback must be harmless.
		if err := p.Stop(); err != nil {
			stopErr.Store(err)
		}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	psDispatch(id) // synchronous: callback has returned once this returns.

	if got := atomic.LoadInt32(&stops); got != 1 {
		t.Fatalf("callback runs = %d, want 1", got)
	}
	if v := stopErr.Load(); v != nil {
		t.Fatalf("Stop inside callback returned error: %v", v)
	}

	// Stop from the caller goroutine afterwards is idempotent and nil.
	if err := p.Stop(); err != nil {
		t.Fatalf("outer Stop: %v", err)
	}
	em.Detach(id)
}

func TestPSReleaseInsideCallback(t *testing.T) {
	p := psNewPlayer(t)

	em, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}

	var releases int32
	var releaseErr atomic.Value
	id, err := em.Attach(MediaPlayerTimeChanged, func(ev Event, ud interface{}) {
		atomic.AddInt32(&releases, 1)
		if err := p.Release(); err != nil {
			releaseErr.Store(err)
		}
		// Re-entrant repeated release must not panic/double-free.
		if err := p.Release(); err != nil {
			releaseErr.Store(err)
		}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	psDispatch(id)
	if got := atomic.LoadInt32(&releases); got != 1 {
		t.Fatalf("callback runs = %d, want 1", got)
	}
	if v := releaseErr.Load(); v != nil {
		t.Fatalf("Release inside callback error: %v", v)
	}

	// The re-entrant release defers native teardown until the dispatch unwinds.
	p.mu.Lock()
	done := p.releaseDone
	p.mu.Unlock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("re-entrant Release did not complete native teardown")
	}

	// After release, the subscription context was removed and late events do
	// not re-enter user code.
	psDispatch(id)
	if got := atomic.LoadInt32(&releases); got != 1 {
		t.Fatalf("callback re-ran after Release: count %d", got)
	}

	// Repeated Release from any goroutine is a safe no-op.
	if err := p.Release(); err != nil {
		t.Fatalf("second Release: %v", err)
	}
}

func TestPSReleaseNoCallbacksAfterRelease(t *testing.T) {
	p := psNewPlayer(t)

	em, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}
	id, calls := psAttachCounter(t, em, MediaPlayerTimeChanged)

	psDispatch(id)
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Fatalf("before release calls = %d, want 1", got)
	}

	psRelease(t, p)

	for i := 0; i < 5; i++ {
		psDispatch(id)
	}
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Fatalf("user callback entered %d times after Release, want 1", got)
	}
}

func TestPSReleaseIdempotentUnderConcurrency(t *testing.T) {
	p := psNewPlayer(t)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Release()
		}()
	}
	wg.Wait()

	p.mu.Lock()
	done := p.releaseDone
	p.mu.Unlock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("release did not complete")
	}

	// Further releases, including on the test goroutine, remain safe.
	for i := 0; i < 4; i++ {
		if err := p.Release(); err != nil {
			t.Fatalf("repeat Release %d: %v", i, err)
		}
	}
}

// ---------------------------------------------------------------------------
// A3: Detach idempotency.
// ---------------------------------------------------------------------------

func TestPSEventManagerDoubleDetachSafe(t *testing.T) {
	p := psNewPlayer(t)
	defer psRelease(t, p)

	em, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}
	id, calls := psAttachCounter(t, em, MediaPlayerTimeChanged)

	psDispatch(id)
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}

	// Repeated and duplicate-ID detaches must not panic.
	em.Detach(id)
	em.Detach(id)
	em.Detach(id, id, 0, EventID(0), 1<<40)

	psDispatch(id)
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Fatalf("callback still active after Detach: count %d", got)
	}

	// Detaching an unknown ID on a zero manager must also be safe.
	zeroEM := &EventManager{}
	zeroEM.Detach(1 << 41)
}

func TestPSEventManagerConcurrentDetach(t *testing.T) {
	p := psNewPlayer(t)
	defer psRelease(t, p)

	em, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}
	id, calls := psAttachCounter(t, em, MediaPlayerTimeChanged)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			em.Detach(id)
		}()
	}
	wg.Wait()
	em.Detach(id)

	psDispatch(id)
	if got := atomic.LoadInt32(calls); got != 0 {
		t.Fatalf("detached callback count = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// A4: volume / mute persist across media switches.
// ---------------------------------------------------------------------------

func TestPSPlayerSettingsPersistAcrossSwitch(t *testing.T) {
	p := psNewPlayer(t)
	defer psRelease(t, p)

	const wantVolume = 63
	if err := p.SetVolume(wantVolume); err != nil {
		t.Fatalf("SetVolume: %v", err)
	}
	if err := p.SetMute(true); err != nil {
		t.Fatalf("SetMute: %v", err)
	}

	if v, _ := p.Volume(); v != wantVolume {
		t.Fatalf("Volume before switch = %d, want %d", v, wantVolume)
	}
	if m, _ := p.IsMuted(); !m {
		t.Fatal("IsMuted before switch = false, want true")
	}

	m1 := psMediaBytes(t, []byte("media-one"))
	if err := p.SetMedia(m1); err != nil {
		t.Fatal(err)
	}
	if v, _ := p.Volume(); v != wantVolume {
		t.Fatalf("Volume after first switch = %d, want %d", v, wantVolume)
	}
	if m, _ := p.IsMuted(); !m {
		t.Fatal("IsMuted after first switch = false, want true")
	}

	m2 := psMediaBytes(t, []byte("media-two-longer"))
	if err := p.SetMedia(m2); err != nil {
		t.Fatal(err)
	}
	if v, _ := p.Volume(); v != wantVolume {
		t.Fatalf("Volume after second switch = %d, want %d", v, wantVolume)
	}
	if m, _ := p.IsMuted(); !m {
		t.Fatal("IsMuted after second switch = false, want true")
	}

	// Loading through LoadMediaFromReadSeeker must preserve settings too.
	m3, err := p.LoadMediaFromReadSeeker(bytes.NewReader([]byte("via-load")))
	if err != nil {
		t.Fatalf("LoadMediaFromReadSeeker: %v", err)
	}
	if v, _ := p.Volume(); v != wantVolume {
		t.Fatalf("Volume after LoadMedia = %d, want %d", v, wantVolume)
	}
	if m, _ := p.IsMuted(); !m {
		t.Fatal("IsMuted after LoadMedia = false, want true")
	}

	// Explicit modification still wins and does not disturb mute.
	if err := p.SetVolume(42); err != nil {
		t.Fatal(err)
	}
	if v, _ := p.Volume(); v != 42 {
		t.Fatalf("Volume after change = %d, want 42", v)
	}
	if m, _ := p.IsMuted(); !m {
		t.Fatal("mute lost after volume change")
	}
	if err := p.SetMute(false); err != nil {
		t.Fatal(err)
	}
	if m, _ := p.IsMuted(); m {
		t.Fatal("IsMuted after explicit unmute = true, want false")
	}
	if v, _ := p.Volume(); v != 42 {
		t.Fatalf("volume changed after unmute: %d", v)
	}

	m1.Release()
	m2.Release()
	m3.Release()
}

// ---------------------------------------------------------------------------
// A5 / A6: reader lifetime and Duplicate sharing.
// ---------------------------------------------------------------------------

var psErrReaderDenied = errors.New("ps reader accessed after release")

type psSpyReadSeeker struct {
	mu          sync.Mutex
	r           *bytes.Reader
	reads       int
	seeks       int
	denied      bool
	deniedCalls int
}

func newPSSpyReader(data []byte) *psSpyReadSeeker {
	return &psSpyReadSeeker{r: bytes.NewReader(data)}
}

func (s *psSpyReadSeeker) Read(p []byte) (int, error) {
	s.mu.Lock()
	if s.denied {
		s.deniedCalls++
		s.mu.Unlock()
		return 0, psErrReaderDenied
	}
	s.reads++
	s.mu.Unlock()
	return s.r.Read(p)
}

func (s *psSpyReadSeeker) Seek(offset int64, whence int) (int64, error) {
	s.mu.Lock()
	if s.denied {
		s.deniedCalls++
		s.mu.Unlock()
		return 0, psErrReaderDenied
	}
	s.seeks++
	s.mu.Unlock()
	return s.r.Seek(offset, whence)
}

func (s *psSpyReadSeeker) setDenied(denied bool) {
	s.mu.Lock()
	s.denied = denied
	s.mu.Unlock()
}

func (s *psSpyReadSeeker) counts() (reads, seeks, deniedCalls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads, s.seeks, s.deniedCalls
}

func psMediaReaderID(t *testing.T, m *Media) objectID {
	t.Helper()
	_, data := m.getUserData()
	if data == nil {
		t.Fatal("media has no user data / reader context")
	}
	if data.readerID == nil {
		t.Fatal("media user data has no reader ID")
	}
	return data.readerID
}

func psReaderRefs(rid objectID) uint {
	inst.objects.RLock()
	defer inst.objects.RUnlock()
	ctx, ok := inst.objects.contexts[rid]
	if !ok {
		return 0
	}
	return ctx.refs
}

func psOpenCallback(t *testing.T, rid objectID) int {
	t.Helper()
	_, ok := mediaBufferOpen(rid)
	if !ok {
		return 1
	}
	return 0
}

func psReadCallback(t *testing.T, rid objectID, size int) int {
	t.Helper()
	buf := make([]byte, size)
	return mediaBufferRead(rid, buf)
}

func TestPSReaderDeniedAfterMediaRelease(t *testing.T) {
	psEnsureVLC(t)

	spy := newPSSpyReader(bytes.Repeat([]byte("r"), 8192))
	m, err := NewMediaFromReadSeeker(spy)
	if err != nil {
		t.Fatal(err)
	}
	rid := psMediaReaderID(t, m)

	if refs := psReaderRefs(rid); refs != 1 {
		t.Fatalf("reader refs after create = %d, want 1", refs)
	}

	// Real production callbacks reach the in-memory reader.
	if rc := psOpenCallback(t, rid); rc != 0 {
		t.Fatalf("open callback before release = %d, want 0", rc)
	}
	if n := psReadCallback(t, rid, 64); n <= 0 {
		t.Fatalf("read callback before release = %d, want > 0", n)
	}
	readsBefore, _, _ := spy.counts()
	if readsBefore == 0 {
		t.Fatal("expected reader.Read to be accessed before release")
	}

	// Release deregisters the reader exactly once.
	if err := m.Release(); err != nil {
		t.Fatalf("Media.Release: %v", err)
	}
	if err := m.Release(); err != nil {
		t.Fatalf("duplicate Media.Release: %v", err)
	}
	if refs := psReaderRefs(rid); refs != 0 {
		t.Fatalf("reader refs after release = %d, want 0 (deregistered)", refs)
	}

	// Any late native callback must fail through the registry and must not
	// touch the released reader.
	spy.setDenied(true)
	_, _, deniedBefore := spy.counts()

	if rc := psOpenCallback(t, rid); rc == 0 {
		t.Fatal("late open callback after Release unexpectedly succeeded")
	}
	if n := psReadCallback(t, rid, 64); n != -1 {
		t.Fatalf("late read callback = %d, want -1 error", n)
	}
	if mediaBufferSeek(rid, 0) {
		t.Fatal("late seek callback unexpectedly succeeded")
	}

	readsAfter, _, deniedAfter := spy.counts()
	if deniedAfter != deniedBefore {
		t.Fatalf("released reader was accessed by late callbacks (denied calls %d -> %d)",
			deniedBefore, deniedAfter)
	}
	if readsAfter != readsBefore {
		t.Fatalf("reader.Read called after deregistration: %d -> %d", readsBefore, readsAfter)
	}
}

func TestPSDuplicateSharesReaderLifetime(t *testing.T) {
	psEnsureVLC(t)

	spy := newPSSpyReader(bytes.Repeat([]byte("s"), 8192))
	original, err := NewMediaFromReadSeeker(spy)
	if err != nil {
		t.Fatal(err)
	}
	rid := psMediaReaderID(t, original)

	dup, err := original.Duplicate()
	if err != nil {
		t.Fatalf("Duplicate: %v", err)
	}
	if rid2 := psMediaReaderID(t, dup); rid2 != rid {
		t.Fatal("duplicate must share the same reader ID")
	}
	if refs := psReaderRefs(rid); refs != 2 {
		t.Fatalf("reader refs after Duplicate = %d, want 2", refs)
	}

	// Release the original: the duplicate keeps the reader alive and the
	// production read callback still succeeds through the shared ID.
	if err := original.Release(); err != nil {
		t.Fatal(err)
	}
	if refs := psReaderRefs(rid); refs != 1 {
		t.Fatalf("reader refs after releasing original = %d, want 1", refs)
	}
	if err := dup.assertInit(); err != nil {
		t.Fatalf("duplicate not initialised after original release: %v", err)
	}
	if rc := psOpenCallback(t, rid); rc != 0 {
		t.Fatalf("open via duplicate after original release = %d, want 0", rc)
	}
	if n := psReadCallback(t, rid, 128); n <= 0 {
		t.Fatalf("read via duplicate after original release = %d, want > 0", n)
	}

	// Releasing the duplicate drops the final reference; late callbacks fail.
	if err := dup.Release(); err != nil {
		t.Fatal(err)
	}
	if refs := psReaderRefs(rid); refs != 0 {
		t.Fatalf("reader refs after releasing duplicate = %d, want 0", refs)
	}
	if n := psReadCallback(t, rid, 32); n != -1 {
		t.Fatalf("read after both releases = %d, want -1", n)
	}
}

func TestPSDuplicateReverseReleaseOrder(t *testing.T) {
	psEnsureVLC(t)

	spy := newPSSpyReader(bytes.Repeat([]byte("z"), 4096))
	original, err := NewMediaFromReadSeeker(spy)
	if err != nil {
		t.Fatal(err)
	}
	rid := psMediaReaderID(t, original)

	dup, err := original.Duplicate()
	if err != nil {
		t.Fatal(err)
	}
	if refs := psReaderRefs(rid); refs != 2 {
		t.Fatalf("refs = %d, want 2", refs)
	}

	// Release the duplicate first; the original must keep reading.
	if err := dup.Release(); err != nil {
		t.Fatal(err)
	}
	if refs := psReaderRefs(rid); refs != 1 {
		t.Fatalf("refs after dup release = %d, want 1", refs)
	}
	if n := psReadCallback(t, rid, 64); n <= 0 {
		t.Fatalf("original read after dup release = %d, want > 0", n)
	}

	if err := original.Release(); err != nil {
		t.Fatal(err)
	}
	if n := psReadCallback(t, rid, 16); n != -1 {
		t.Fatalf("read after original release = %d, want -1", n)
	}
}

// ---------------------------------------------------------------------------
// End to end: real playback of generated in-memory WAV data across a switch.
// ---------------------------------------------------------------------------

// psMakeWAV builds a tiny mono 16-bit PCM WAV of silence in memory. Silence is
// enough for the demuxer/output pipeline and avoids depending on any media
// file or decoder fixture.
func psMakeWAV(milliseconds int) []byte {
	const sampleRate = 8000
	samples := sampleRate * milliseconds / 1000
	pcm := make([]byte, samples*2)

	var buf bytes.Buffer
	buf.WriteString("RIFF")
	_ = psWriteU32(&buf, uint32(36+len(pcm)))
	buf.WriteString("WAVEfmt ")
	_ = psWriteU32(&buf, 16)
	_ = psWriteU16(&buf, 1) // PCM
	_ = psWriteU16(&buf, 1) // mono
	_ = psWriteU32(&buf, sampleRate)
	_ = psWriteU32(&buf, sampleRate*2)
	_ = psWriteU16(&buf, 2) // block align
	_ = psWriteU16(&buf, 16)
	buf.WriteString("data")
	_ = psWriteU32(&buf, uint32(len(pcm)))
	buf.Write(pcm)
	return buf.Bytes()
}

func psWriteU32(buf *bytes.Buffer, v uint32) error {
	b := []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
	_, err := buf.Write(b)
	return err
}

func psWriteU16(buf *bytes.Buffer, v uint16) error {
	_, err := buf.Write([]byte{byte(v), byte(v >> 8)})
	return err
}

func TestPSSyntheticWAVSwitchPlaysAndEnds(t *testing.T) {
	if os.Getenv("PS_SKIP_PLAYBACK") == "1" {
		t.Skip("PS_SKIP_PLAYBACK=1")
	}

	p := psNewPlayer(t)
	defer psRelease(t, p)

	end1 := make(chan struct{}, 1)
	end2 := make(chan struct{}, 1)

	m1, err := p.LoadMediaFromReadSeeker(bytes.NewReader(psMakeWAV(300)))
	if err != nil {
		t.Fatalf("load first wav: %v", err)
	}
	defer m1.Release()

	em, err := p.EventManager()
	if err != nil {
		t.Fatal(err)
	}
	id1, err := em.Attach(MediaPlayerEndReached, func(Event, interface{}) {
		select {
		case end1 <- struct{}{}:
		default:
		}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Play(); err != nil {
		t.Fatalf("play first: %v", err)
	}
	select {
	case <-end1:
		t.Log("first synthetic media reached EndReached")
	case <-time.After(30 * time.Second):
		st, _ := p.MediaState()
		t.Fatalf("first media did not end in time, state=%v", st)
	}

	// Switch to a second generated media. The first session's id1 is now
	// stale; attach a fresh callback for the second session.
	m2, err := p.LoadMediaFromReadSeeker(bytes.NewReader(psMakeWAV(250)))
	if err != nil {
		t.Fatalf("load second wav: %v", err)
	}
	defer m2.Release()

	id2, err := em.Attach(MediaPlayerEndReached, func(Event, interface{}) {
		select {
		case end2 <- struct{}{}:
		default:
		}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Play(); err != nil {
		t.Fatalf("play second: %v", err)
	}
	select {
	case <-end2:
		t.Log("second synthetic media reached EndReached")
	case <-time.After(30 * time.Second):
		st, _ := p.MediaState()
		t.Fatalf("second media did not end in time, state=%v", st)
	}

	// The stale first-session callback must not have been re-triggered by the
	// second playback.
	select {
	case <-end1:
		t.Fatal("stale first-session EndReached callback fired during second playback")
	default:
	}

	em.Detach(id1, id2)
}
