package proxy

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMinimalSingleFlightMechanism proves the close-broadcast single-flight
// pattern used by ensureRefresh: a leader holds the refresh, every concurrent
// caller joins the same call and is released together when done closes, and
// only one refresh ever runs regardless of how many callers join.
func TestMinimalSingleFlightMechanism(t *testing.T) {
	type res struct{ v int }
	type call struct {
		done chan struct{}
		res  res
	}
	var mu sync.Mutex
	var inflight *call
	var calls atomic.Int32
	leaderInRefresh := make(chan struct{})
	release := make(chan struct{})

	run := func() res {
		mu.Lock()
		if c := inflight; c != nil {
			mu.Unlock()
			<-c.done
			return c.res
		}
		c := &call{done: make(chan struct{})}
		inflight = c
		mu.Unlock()

		calls.Add(1)
		close(leaderInRefresh)
		<-release
		c.res = res{v: 42}

		mu.Lock()
		inflight = nil
		mu.Unlock()
		close(c.done)
		return c.res
	}

	leaderResult := make(chan res, 1)
	go func() { leaderResult <- run() }()
	<-leaderInRefresh

	const joiners = 15
	var wg sync.WaitGroup
	out := make([]res, joiners)
	for i := range joiners {
		wg.Add(1)
		go func(i int) { defer wg.Done(); out[i] = run() }(i)
	}
	time.Sleep(30 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		close(release)
		t.Fatalf("refresh calls=%d before release, want 1", got)
	}

	close(release)
	if r := <-leaderResult; r.v != 42 {
		t.Fatalf("leader result=%d", r.v)
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh calls=%d, want exactly 1 (single-flight)", got)
	}
	for i, r := range out {
		if r.v != 42 {
			t.Fatalf("joiner %d got %d, want 42", i, r.v)
		}
	}
}
