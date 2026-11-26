package cmd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alfg/ffmpegd/ffmpeg"
)

// statusLog records every status the session broadcasts, in order.
type statusLog struct {
	mu   sync.Mutex
	msgs []Status
	wake chan struct{}
}

func newStatusLog() *statusLog {
	return &statusLog{wake: make(chan struct{}, 1)}
}

func (l *statusLog) broadcast(st Status) {
	l.mu.Lock()
	l.msgs = append(l.msgs, st)
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *statusLog) snapshot() []Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Status(nil), l.msgs...)
}

// waitFor blocks until cond holds for the recorded statuses.
func (l *statusLog) waitFor(t *testing.T, what string, cond func([]Status) bool) []Status {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		got := l.snapshot()
		if cond(got) {
			return got
		}
		select {
		case <-l.wake:
		case <-deadline:
			t.Fatalf("timed out waiting for %s; statuses so far: %+v", what, got)
			return nil
		}
	}
}

// job is one test task and the statuses recorded for it.
type jobLog struct {
	id uint64
}

func jobStatuses(all []Status, id uint64) []Status {
	var out []Status
	for _, st := range all {
		if st.JobID == id {
			out = append(out, st)
		}
	}
	return out
}

func isFinal(st Status) bool {
	return st.Cancelled || st.Err != "" || st.Percent == 100
}

func finals(all []Status) []Status {
	var out []Status
	for _, st := range all {
		if isFinal(st) {
			out = append(out, st)
		}
	}
	return out
}

// startSession creates a session whose encoder is fn, and tears it down after
// the test.
func startSession(t *testing.T, fn encodeFunc) (*session, *statusLog) {
	t.Helper()
	log := newStatusLog()
	s := newSession(10, fn, log.broadcast)
	t.Cleanup(s.shutdown)
	return s, log
}

// shutdown stops the worker and waits for it, so no goroutine outlives a test.
func (s *session) shutdown() {
	s.close()
	s.wait()
}

func TestSessionCancelOnlyAffectsRunningJob(t *testing.T) {
	started := make(chan string, 3)

	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		started <- msg.Input
		if msg.Input == "a" {
			report(Status{Percent: 10})
			<-ctx.Done()
			// A late report from the cancelled task must never reach clients.
			report(Status{Percent: 50})
			return ffmpeg.ErrCancelled
		}
		report(Status{Percent: 20})
		return nil
	}
	s, log := startSession(t, encode)

	for _, name := range []string{"a", "b", "c"} {
		if err := s.submit(Message{Type: "encode", Input: name}); err != nil {
			t.Fatal(err)
		}
	}

	if got := <-started; got != "a" {
		t.Fatalf("first job started = %q, want a", got)
	}
	// Wait until the running job's progress is on the wire before cancelling, so
	// the test asserts the order progress-then-cancel rather than racing it.
	log.waitFor(t, "job 1 progress", func(all []Status) bool {
		return len(jobStatuses(all, 1)) == 1
	})
	if !s.cancel() {
		t.Fatal("cancel of the running job was not accepted")
	}

	// Queued jobs must still run, in submission order, after the cancel.
	if got := <-started; got != "b" {
		t.Fatalf("second job started = %q, want b", got)
	}
	if got := <-started; got != "c" {
		t.Fatalf("third job started = %q, want c", got)
	}

	all := log.waitFor(t, "all three jobs to finish", func(all []Status) bool {
		return len(finals(all)) == 3
	})

	// Job 1: progress, exactly one cancellation, and the stale 50% dropped.
	j1 := jobStatuses(all, 1)
	if len(j1) != 2 {
		t.Fatalf("job 1 statuses = %+v, want progress then one cancellation", j1)
	}
	if j1[0].Percent != 10 || isFinal(j1[0]) {
		t.Fatalf("job 1 first status = %+v, want 10%% progress", j1[0])
	}
	if !j1[1].Cancelled || j1[1].Percent != 0 {
		t.Fatalf("job 1 final status = %+v, want cancellation only", j1[1])
	}

	// Jobs 2 and 3: progress then exactly one 100% completion each.
	for _, id := range []uint64{2, 3} {
		js := jobStatuses(all, id)
		if len(js) != 2 || js[0].Percent != 20 || js[1].Percent != 100 || js[1].Cancelled || js[1].Err != "" {
			t.Fatalf("job %d statuses = %+v, want 20%% then 100%%", id, js)
		}
	}

	// No status may report both a cancellation and a completion for one job.
	if len(finals(all)) != 3 {
		t.Fatalf("finals = %+v, want exactly one per job", finals(all))
	}
}

func TestSessionCancelBeforeEncoderStarts(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan string, 2)

	// This mirrors encodeJob: it checks for cancellation before starting the
	// ffmpeg process, so a cancel accepted while the job is current is honoured
	// even though nothing has been launched yet.
	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		started <- msg.Input
		if msg.Input == "first" {
			<-gate
			if ctx.Err() != nil {
				return ffmpeg.ErrCancelled
			}
			return nil
		}
		if ctx.Err() != nil {
			return ffmpeg.ErrCancelled
		}
		report(Status{Percent: 20})
		return nil
	}
	s, log := startSession(t, encode)

	if err := s.submit(Message{Type: "encode", Input: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := s.submit(Message{Type: "encode", Input: "second"}); err != nil {
		t.Fatal(err)
	}

	if got := <-started; got != "first" {
		t.Fatalf("first job started = %q", got)
	}
	// The job has been dequeued but is blocked before "starting ffmpeg".
	// The cancel must be accepted here, not lost.
	if !s.cancel() {
		t.Fatal("cancel arriving before ffmpeg starts was lost")
	}
	close(gate)

	if got := <-started; got != "second" {
		t.Fatalf("second job started = %q, want second", got)
	}

	all := log.waitFor(t, "both jobs to finalize", func(all []Status) bool {
		return len(finals(all)) == 2
	})

	j1 := jobStatuses(all, 1)
	if len(j1) != 1 || !j1[0].Cancelled {
		t.Fatalf("job 1 statuses = %+v, want a single cancellation", j1)
	}

	// The cancel must not have touched the next job.
	j2 := jobStatuses(all, 2)
	if len(j2) != 2 || j2[0].Percent != 20 || j2[1].Percent != 100 {
		t.Fatalf("job 2 statuses = %+v, want unaffected progress then completion", j2)
	}
}

func TestSessionLateProgressDroppedAfterJobEnds(t *testing.T) {
	var staleReport func(Status)
	started := make(chan string, 2)
	reported := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }

	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		switch msg.Input {
		case "old":
			staleReport = report
			return nil // finishes, then the old progress reporter fires late
		case "new":
			started <- msg.Input
			report(Status{Percent: 20})
			reported <- struct{}{}
			<-release
			return nil
		}
		return nil
	}
	// Create the session by hand so the encoder is unblocked before shutdown
	// waits on it, even when an assertion fails.
	log := newStatusLog()
	s := newSession(10, encode, log.broadcast)
	t.Cleanup(s.shutdown)
	t.Cleanup(unblock) // runs before shutdown, since cleanups are LIFO

	if err := s.submit(Message{Type: "encode", Input: "old"}); err != nil {
		t.Fatal(err)
	}
	log.waitFor(t, "old job to complete", func(all []Status) bool {
		return len(finals(all)) == 1 && finals(all)[0].JobID == 1 && finals(all)[0].Percent == 100
	})

	if err := s.submit(Message{Type: "encode", Input: "new"}); err != nil {
		t.Fatal(err)
	}
	if got := <-started; got != "new" {
		t.Fatalf("started = %q, want new", got)
	}
	<-reported // the new job's own progress is on the wire before the stale call

	// The old task's reporter fires now, while the next task is running. The
	// call is synchronous, so once it returns the status is either recorded or
	// was dropped; no sleep is needed to prove it.
	staleReport(Status{Percent: 77})

	for _, st := range log.snapshot() {
		if st.Percent == 77 {
			t.Fatalf("late progress from the finished job was broadcast: %+v", st)
		}
		if st.JobID == 1 && st.Percent != 100 {
			t.Fatalf("old job produced a status after its completion: %+v", st)
		}
	}
	if got := jobStatuses(log.snapshot(), 2); len(got) != 1 || got[0].Percent != 20 {
		t.Fatalf("new job statuses before completion = %+v, want only its own 20%%", got)
	}

	unblock()
	all := log.waitFor(t, "new job to complete", func(all []Status) bool {
		return len(finals(all)) == 2
	})
	if got := jobStatuses(all, 2); len(got) != 2 || got[1].Percent != 100 || got[1].Cancelled {
		t.Fatalf("new job statuses = %+v, want 20%% then 100%%", got)
	}
}

func TestSessionFailedJobReportsOneError(t *testing.T) {
	boom := errors.New("boom: invalid encoder")
	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		return boom
	}
	s, log := startSession(t, encode)

	if err := s.submit(Message{Type: "encode", Input: "x"}); err != nil {
		t.Fatal(err)
	}
	all := log.waitFor(t, "failure status", func(all []Status) bool { return len(all) > 0 })

	if len(all) != 1 {
		t.Fatalf("statuses = %+v, want exactly one error", all)
	}
	if all[0].Err != boom.Error() || all[0].Percent != 0 || all[0].Cancelled {
		t.Fatalf("status = %+v, want the error only", all[0])
	}
}

func TestSessionSuccessReportsOneHundred(t *testing.T) {
	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		report(Status{Percent: 40})
		report(Status{Percent: 80})
		return nil
	}
	s, log := startSession(t, encode)

	if err := s.submit(Message{Type: "encode"}); err != nil {
		t.Fatal(err)
	}
	all := log.waitFor(t, "completion", func(all []Status) bool { return len(finals(all)) == 1 })

	got := jobStatuses(all, 1)
	if len(got) != 3 || got[2].Percent != 100 || got[2].Cancelled || got[2].Err != "" {
		t.Fatalf("statuses = %+v, want 40, 80, then a single 100", got)
	}
}

func TestSessionCancelWithNoRunningJob(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }

	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		<-release
		return nil
	}
	// Create the session by hand so the encoder is unblocked before shutdown
	// waits on it, even when an assertion fails.
	log := newStatusLog()
	s := newSession(10, encode, log.broadcast)
	t.Cleanup(s.shutdown)
	t.Cleanup(unblock) // runs before shutdown, since cleanups are LIFO

	// Nothing running yet: cancel is a no-op, and must not affect a job queued
	// after it.
	if s.cancel() {
		t.Fatal("cancel reported a running job when the queue was idle")
	}
	if err := s.submit(Message{Type: "encode"}); err != nil {
		t.Fatal(err)
	}
	unblock()

	all := log.waitFor(t, "completion", func(all []Status) bool { return len(finals(all)) == 1 })
	if all[0].Percent != 100 || all[0].Cancelled {
		t.Fatalf("status = %+v, want completion", all[0])
	}
	if s.cancel() {
		t.Fatal("cancel reported a running job after the queue drained")
	}
}

func TestSessionQueueOrder(t *testing.T) {
	var order []string
	var mu sync.Mutex
	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		mu.Lock()
		order = append(order, msg.Input)
		mu.Unlock()
		return nil
	}
	s, log := startSession(t, encode)

	for _, name := range []string{"one", "two", "three"} {
		if err := s.submit(Message{Type: "encode", Input: name}); err != nil {
			t.Fatal(err)
		}
	}
	log.waitFor(t, "all jobs", func(all []Status) bool { return len(finals(all)) == 3 })

	mu.Lock()
	defer mu.Unlock()
	want := []string{"one", "two", "three"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("execution order = %v, want %v", order, want)
		}
	}
}
