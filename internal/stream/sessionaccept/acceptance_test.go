// Package sessionaccept contains black-box acceptance tests for shared
// playback session recycling and channel switching. It lives in its own
// package so it cannot collide with white-box tests added to package stream.
package sessionaccept

import (
	"antennarr/internal/stream"
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !fn() {
		t.Fatalf("timed out waiting for: %s", what)
	}
}

func recvOne(t *testing.T, ch <-chan []byte) {
	t.Helper()
	select {
	case b := <-ch:
		if len(b) == 0 {
			t.Fatal("empty media")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no media")
	}
}

type invocation struct {
	id    string
	enter chan struct{}
	exit  chan struct{}
}

// gateRunner records every upstream invocation, tracks per-channel
// concurrency, and optionally blocks runners in post-cancel cleanup.
type gateRunner struct {
	starts atomic.Int32

	cleanup chan struct{} // optional global gate after ctx cancellation

	mu       sync.Mutex
	invoc    []*invocation
	current  map[string]int
	maxPerID map[string]int
}

func newGateRunner(cleanup chan struct{}) *gateRunner {
	return &gateRunner{cleanup: cleanup, current: map[string]int{}, maxPerID: map[string]int{}}
}

func (g *gateRunner) fn(ctx context.Context, id string, emit func([]byte)) error {
	inv := &invocation{id: id, enter: make(chan struct{}), exit: make(chan struct{})}
	g.mu.Lock()
	g.invoc = append(g.invoc, inv)
	g.starts.Add(1)
	g.current[id]++
	if g.current[id] > g.maxPerID[id] {
		g.maxPerID[id] = g.current[id]
	}
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.current[id]--
		g.mu.Unlock()
	}()
	close(inv.enter)
	emit([]byte{0x47})
	tk := time.NewTicker(time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			if g.cleanup != nil {
				<-g.cleanup
			}
			close(inv.exit)
			return nil
		case <-tk.C:
			emit([]byte{0x47})
		}
	}
}

func (g *gateRunner) last(id string) *invocation {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := len(g.invoc) - 1; i >= 0; i-- {
		if g.invoc[i].id == id {
			return g.invoc[i]
		}
	}
	return nil
}

func (g *gateRunner) maxConcurrentFor(id string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maxPerID[id]
}

// A1: single viewer plays; release zeroes viewers and cancels/exits runner.
func TestA1SingleViewerCleanup(t *testing.T) {
	var exited atomic.Int32
	m := stream.New(2, 5*time.Millisecond, func(ctx context.Context, _ string, emit func([]byte)) error {
		emit([]byte{1})
		<-ctx.Done()
		exited.Add(1)
		return nil
	})
	defer m.Close()
	sub, release, err := m.Subscribe("a", "720")
	if err != nil {
		t.Fatal(err)
	}
	recvOne(t, sub)
	if m.Status().Viewers != 1 {
		t.Fatal("viewer not counted")
	}
	release()
	waitUntil(t, time.Second, func() bool { return exited.Load() == 1 }, "runner exit")
	waitUntil(t, time.Second, func() bool { s := m.Status(); return s.Viewers == 0 && s.Channels == 0 }, "zero status")
}

// A2: two viewers share one upstream; it survives the first leave and is
// reaped after the grace period following the last leave.
func TestA2SharedUpstreamGrace(t *testing.T) {
	g := newGateRunner(nil)
	m := stream.New(4, 25*time.Millisecond, g.fn)
	defer m.Close()
	a, ra, err := m.Subscribe("a", "a")
	if err != nil {
		t.Fatal(err)
	}
	b, rb, err := m.Subscribe("a", "a")
	if err != nil {
		t.Fatal(err)
	}
	recvOne(t, a)
	recvOne(t, b)
	if g.starts.Load() != 1 {
		t.Fatal("upstream not shared")
	}
	ra()
	if m.Status().Viewers != 1 {
		t.Fatal("first release count")
	}
	time.Sleep(60 * time.Millisecond) // beyond grace
	if g.starts.Load() != 1 {
		t.Fatal("grace reaped while a viewer remained")
	}
	rb()
	waitUntil(t, time.Second, func() bool { return m.Status().Channels == 0 }, "grace reap")
}

// A3: a slow evicted viewer decrements the count; fast viewer keeps media.
func TestA3SlowViewerEviction(t *testing.T) {
	step := make(chan struct{})
	m := stream.New(3, time.Millisecond, func(ctx context.Context, _ string, emit func([]byte)) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-step:
				emit([]byte{0x47})
			}
		}
	})
	defer m.Close()
	slow, rs, _ := m.Subscribe("a", "a")
	defer rs()
	fast, rf, _ := m.Subscribe("a", "a")
	defer rf()
	for i := 0; i < 4096+4; i++ {
		step <- struct{}{}
		select {
		case <-fast:
		case <-time.After(2 * time.Second):
			t.Fatal("fast viewer blocked")
		}
	}
	waitUntil(t, time.Second, func() bool { return m.Status().Viewers == 1 }, "slow eviction count")
	rf()
	rs()
	waitUntil(t, time.Second, func() bool { return m.Status().Viewers == 0 }, "final zero")
	_ = slow
}

// A5: re-entering within grace reuses the running upstream.
func TestA5GraceReentryReuses(t *testing.T) {
	var starts atomic.Int32
	m := stream.New(4, time.Second, func(ctx context.Context, _ string, emit func([]byte)) error {
		starts.Add(1)
		tk := time.NewTicker(time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-tk.C:
				emit([]byte{1})
			}
		}
	})
	defer m.Close()
	first, r1, _ := m.Subscribe("a", "a")
	recvOne(t, first)
	r1()
	time.Sleep(10 * time.Millisecond) // well within 1s grace
	again, r2, err := m.Subscribe("a", "a")
	if err != nil {
		t.Fatal(err)
	}
	defer r2()
	recvOne(t, again)
	if starts.Load() != 1 {
		t.Fatalf("grace reentry started %d upstreams", starts.Load())
	}
}

// A6: Stop closes only the targeted channel and waits for its runner; the
// other channel keeps streaming and its count is untouched.
func TestA6StopIsolationAndSynchronousExit(t *testing.T) {
	g := newGateRunner(nil)
	m := stream.New(4, time.Hour, g.fn)
	defer m.Close()
	a, ra, _ := m.Subscribe("a", "a")
	defer ra()
	b1, rb1, _ := m.Subscribe("b", "b")
	b2, rb2, _ := m.Subscribe("b", "b")
	defer rb1()
	defer rb2()
	recvOne(t, a)
	recvOne(t, b1)
	recvOne(t, b2)
	invA := g.last("a")
	if invA == nil {
		t.Fatal("a runner missing")
	}
	if !m.Stop("a") {
		t.Fatal("Stop missed")
	}
	select {
	case <-invA.exit:
	case <-time.After(time.Second):
		t.Fatal("Stop returned before upstream exited")
	}
	if s := m.Status(); s.Viewers != 2 || s.Channels != 1 {
		t.Fatalf("after Stop: %+v", s)
	}
	select {
	case <-b1:
	case <-time.After(2 * time.Second):
		t.Fatal("other channel affected")
	}
	if m.Stop("a") {
		t.Fatal("second Stop must report false")
	}
}

// A7: Close waits for every runner and reports zero viewers/channels.
func TestA7CloseWaitsAndZeroes(t *testing.T) {
	var exited atomic.Int32
	m := stream.New(4, time.Hour, func(ctx context.Context, _ string, emit func([]byte)) error {
		emit([]byte{1})
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond)
		exited.Add(1)
		return nil
	})
	s1, _, _ := m.Subscribe("a", "a")
	s2, _, _ := m.Subscribe("b", "b")
	recvOne(t, s1)
	recvOne(t, s2)
	done := make(chan struct{})
	go func() { m.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung")
	}
	if exited.Load() != 2 {
		t.Fatalf("exited runners=%d", exited.Load())
	}
	if s := m.Status(); s.Viewers != 0 || s.Channels != 0 {
		t.Fatalf("post-close status %+v", s)
	}
}

// A8: reaching the viewer limit returns ErrBusy and leaves existing viewers.
func TestA8LimitBaseline(t *testing.T) {
	m := stream.New(2, time.Millisecond, func(ctx context.Context, _ string, emit func([]byte)) error {
		emit([]byte{1})
		<-ctx.Done()
		return nil
	})
	defer m.Close()
	_, r1, e := m.Subscribe("a", "a")
	if e != nil {
		t.Fatal(e)
	}
	defer r1()
	_, r2, e := m.Subscribe("b", "b")
	if e != nil {
		t.Fatal(e)
	}
	defer r2()
	if _, _, e := m.Subscribe("c", "c"); !errors.Is(e, stream.ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", e)
	}
	if s := m.Status(); s.Viewers != 2 {
		t.Fatalf("existing viewers affected: %+v", s)
	}
}

// A4: rapid leave/return serializes generations; at most one upstream for
// channel a at a time and the new session receives media after cleanup.
func TestA4RapidReturnSerializes(t *testing.T) {
	cleanup := make(chan struct{})
	g := newGateRunner(cleanup)
	m := stream.New(4, time.Minute, g.fn)
	defer m.Close()

	a, ra, _ := m.Subscribe("a", "a")
	recvOne(t, a)
	ra()
	b, rb, _ := m.Subscribe("b", "b")
	recvOne(t, b)
	rb()
	again, rr, e := m.Subscribe("a", "a")
	if e != nil {
		t.Fatal(e)
	}
	defer rr()

	// New a-generation must not start while the old cleanup gate is held.
	time.Sleep(40 * time.Millisecond)
	if g.starts.Load() != 2 { // a(first) + b
		t.Fatalf("unexpected starts before cleanup: %d", g.starts.Load())
	}
	if inv := g.last("a"); inv == nil {
		t.Fatal("no a invocation")
	}
	close(cleanup)
	recvOne(t, again)
	waitUntil(t, time.Second, func() bool { return g.starts.Load() == 3 }, "second a generation start")
	if mx := g.maxConcurrentFor("a"); mx > 1 {
		t.Fatalf("channel a upstream overlapped, max=%d", mx)
	}
}

// --- Real OS subprocess fake upstream: cancellation must reap the process. ---

func TestHelperProcess41158(t *testing.T) {
	if os.Getenv("ACCEPT41158_HELPER") != "1" {
		return
	}
	buf := []byte{0x47}
	for {
		if _, err := os.Stdout.Write(buf); err != nil {
			os.Exit(0)
		}
		time.Sleep(time.Millisecond)
	}
}

type procTracker struct {
	mu    sync.Mutex
	exits []chan struct{}
}

func (p *procTracker) runner() stream.Runner {
	return func(ctx context.Context, _ string, emit func([]byte)) error {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestHelperProcess41158")
		cmd.Env = append(os.Environ(), "ACCEPT41158_HELPER=1")
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}
		exit := make(chan struct{})
		p.mu.Lock()
		p.exits = append(p.exits, exit)
		p.mu.Unlock()
		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				emit(append([]byte(nil), buf[:n]...))
			}
			if err != nil {
				break
			}
		}
		werr := cmd.Wait()
		close(exit)
		return werr
	}
}

func TestRealSubprocessReapedOnClose(t *testing.T) {
	tr := &procTracker{}
	m := stream.New(4, time.Hour, tr.runner())
	s1, _, e := m.Subscribe("a", "a")
	if e != nil {
		t.Fatal(e)
	}
	recvOne(t, s1)
	m.Close()
	tr.mu.Lock()
	exits := append([]chan struct{}{}, tr.exits...)
	tr.mu.Unlock()
	if len(exits) != 1 {
		t.Fatalf("expected 1 upstream, got %d", len(exits))
	}
	select {
	case <-exits[0]:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream process was not cancelled and waited on")
	}
}
