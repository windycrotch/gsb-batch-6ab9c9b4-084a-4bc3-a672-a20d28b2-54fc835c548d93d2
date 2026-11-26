package forwarder

import (
	"context"
	"sync"
)

// inflightSegment tracks a segment that is being, or has been, processed by a forwarder.
// It guarantees that concurrent submissions of the same segment collapse into a single
// effective processing run and that every submission observes a terminal outcome.
type inflightSegment struct {
	// done is closed once the segment reaches a terminal state (processed / skipped /
	// failed / cancelled)
	done chan struct{}
	// result is the single terminal outcome shared by every submission of this segment
	result SegmentForwardResult
	// waiters count how many submissions are currently referencing this entry
	waiters int
	// completed marks whether the owner run already reached a terminal state
	completed bool
}

// inflightTracker coordinates per-segment single-flight processing, observable failure
// records, and the forwarder shutdown handshake.
type inflightTracker struct {
	lock sync.Mutex
	// entries segments currently in-flight or completed but still retained for dedup
	entries map[string]*inflightSegment
	// failures terminal failure records, keyed by segment identity
	failures map[string]FailedSegmentRecord
	// stopCh closed when the owning forwarder begins shutting down
	stopCh chan struct{}
	// stopped records whether shutdown has begun
	stopped bool
	// inflightWg tracks segments actively being processed (owner runs)
	inflightWg sync.WaitGroup
}

func newInflightTracker() *inflightTracker {
	return &inflightTracker{
		entries:  map[string]*inflightSegment{},
		failures: map[string]FailedSegmentRecord{},
		stopCh:   make(chan struct{}),
	}
}

// beginShutdown marks the tracker as shutting down. Submissions made after this point
// are rejected, and any wait on stopCh unblocks. It is safe to call multiple times.
func (t *inflightTracker) beginShutdown() {
	t.lock.Lock()
	defer t.lock.Unlock()
	if !t.stopped {
		t.stopped = true
		close(t.stopCh)
	}
}

// isStopped reports whether shutdown began
func (t *inflightTracker) isStopped() bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.stopped
}

// acquire reserves a segment for processing.
//
//   - When the segment is unknown, a new entry is created and started == true; the
//     caller owns the single processing run and MUST call finish exactly once.
//   - When the segment is known but still in-flight, started == false and the caller
//     waits on the returned entry's done channel for the shared terminal result.
//   - When the segment already reached a successful / skipped terminal state, started ==
//     false and alreadyDone == true; the caller reuses entry.result without new work, so
//     a segment is never uploaded or broadcast twice.
//   - When the segment previously failed, a fresh processing run is started (started ==
//     true) so an explicit re-drive can retry it.
//   - When shutdown already began, ok == false and no processing is allowed.
func (t *inflightTracker) acquire(
	key string, base SegmentForwardResult,
) (entry *inflightSegment, started bool, alreadyDone bool, ok bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.stopped {
		return nil, false, false, false
	}
	if existing, found := t.entries[key]; found {
		// A previously failed segment may be re-driven for another attempt
		if existing.completed && existing.result.Status == SegmentStatusFailed {
			fresh := &inflightSegment{
				done: make(chan struct{}), waiters: 1,
				result: SegmentForwardResult{
					SegmentID:   base.SegmentID,
					SegmentName: base.SegmentName,
					URI:         base.URI,
				},
			}
			t.entries[key] = fresh
			delete(t.failures, key)
			t.inflightWg.Add(1)
			return fresh, true, false, true
		}
		existing.waiters++
		return existing, false, existing.completed, true
	}
	fresh := &inflightSegment{
		done:    make(chan struct{}),
		waiters: 1,
		result: SegmentForwardResult{
			SegmentID:   base.SegmentID,
			SegmentName: base.SegmentName,
			URI:         base.URI,
		},
	}
	t.entries[key] = fresh
	t.inflightWg.Add(1)
	return fresh, true, false, true
}

// finish records the terminal result of the single owner run, retains any failure for
// observability, and unblocks every concurrent waiter
func (t *inflightTracker) finish(key string, result SegmentForwardResult) {
	t.lock.Lock()
	entry, found := t.entries[key]
	if !found {
		t.lock.Unlock()
		return
	}
	entry.result = result
	if result.Status == SegmentStatusFailed {
		t.failures[key] = FailedSegmentRecord{
			SegmentID:   result.SegmentID,
			SegmentName: result.SegmentName,
			URI:         result.URI,
			Err:         result.Err,
			Attempts:    result.Attempts,
		}
	} else {
		// A later success clears any earlier failure record for the same segment
		delete(t.failures, key)
	}
	entry.completed = true
	close(entry.done)
	t.inflightWg.Done()
	t.lock.Unlock()
}

// release marks that a submission no longer references this segment. Completed entries
// are intentionally retained: they hold the terminal result and continue to de-duplicate
// later submissions of the same segment for the forwarder's lifetime.
func (t *inflightTracker) release(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	existing, found := t.entries[key]
	if !found || existing.waiters <= 0 {
		return
	}
	existing.waiters--
}

// waitForResult blocks until the segment reaches a terminal state, shutdown begins, or
// the context expires
func (t *inflightTracker) waitForResult(
	ctx context.Context, entry *inflightSegment,
) (SegmentForwardResult, error) {
	select {
	case <-entry.done:
		return entry.result, nil
	case <-t.stopCh:
		return SegmentForwardResult{
			SegmentID:   entry.result.SegmentID,
			SegmentName: entry.result.SegmentName,
			URI:         entry.result.URI,
			Status:      SegmentStatusCancelled,
			Decision:    SegmentDecisionProcess,
		}, errForwarderStopping
	case <-ctx.Done():
		return SegmentForwardResult{}, ctx.Err()
	}
}

// waitInflight waits for all in-flight owner runs to finish or the context to expire
func (t *inflightTracker) waitInflight(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		t.inflightWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// abortInflight finalizes every entry whose owner run never reached a terminal state as
// explicitly cancelled. This covers tasks still queued when the worker event loop exits
// during shutdown, so no submission is left waiting forever and no late broadcast can
// occur. Returns the number of entries that were aborted.
func (t *inflightTracker) abortInflight() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	aborted := 0
	for key, entry := range t.entries {
		if entry.completed {
			continue
		}
		entry.result.Status = SegmentStatusCancelled
		entry.result.Decision = SegmentDecisionProcess
		entry.result.Err = nil
		entry.completed = true
		close(entry.done)
		t.inflightWg.Done()
		aborted++
		_ = key
	}
	return aborted
}

// failedRecords returns a snapshot of segments whose retries were exhausted
func (t *inflightTracker) failedRecords() []FailedSegmentRecord {
	t.lock.Lock()
	defer t.lock.Unlock()
	out := make([]FailedSegmentRecord, 0, len(t.failures))
	for _, rec := range t.failures {
		out = append(out, rec)
	}
	return out
}

// clearFailure removes a retained failure record (e.g. after a successful retry)
func (t *inflightTracker) clearFailure(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.failures, key)
}
