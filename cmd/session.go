package cmd

import (
	"context"
	"errors"
	"sync"

	"github.com/alfg/ffmpegd/ffmpeg"
)

// encodeFunc runs one encode and reports progress until it returns.
//
// It must stop promptly and return ffmpeg.ErrCancelled when ctx is cancelled
// before the encode completes. report may be called from any goroutine; calls
// made after cancellation or after encodeFunc returns are ignored.
type encodeFunc func(ctx context.Context, msg Message, report func(Status)) error

// job is one encode selected for execution. It is the identity that ties
// progress and the final status to the task that produced them.
type job struct {
	id     uint64
	msg    Message
	ctx    context.Context
	cancel context.CancelFunc

	// finalized and cancelRequested are guarded by session.emitMu. finalized
	// makes sure a job produces exactly one final status, and no progress after
	// it. cancelRequested stops progress as soon as a cancel is accepted.
	finalized       bool
	cancelRequested bool
}

var errQueueFull = errors.New("too many encodes queued, try again later")

// session queues encode jobs and runs them one at a time.
//
// Two locks keep the status stream consistent:
//
//   - mu guards the queue and current. A job becomes current in the same
//     critical section that removes it from the queue, so a cancel cannot fall
//     between "dequeued" and "started", and cannot reach a queued job.
//   - emitMu serializes every status write against finalization. A progress
//     update from a stale ticker therefore cannot follow, or interleave with,
//     the final status of a job, nor appear among a later job's updates.
type session struct {
	mu      sync.Mutex
	cond    *sync.Cond
	queue   []Message
	current *job
	max     int
	closed  bool
	nextID  uint64

	emitMu    sync.Mutex
	broadcast func(Status)

	encode encodeFunc
	done   chan struct{}
}

func newSession(max int, encode encodeFunc, broadcast func(Status)) *session {
	s := &session{
		max:       max,
		encode:    encode,
		broadcast: broadcast,
		done:      make(chan struct{}),
		nextID:    1, // 0 is reserved for statuses that belong to no job.
	}
	s.cond = sync.NewCond(&s.mu)
	go s.worker()
	return s
}

// submit queues an encode. Jobs run in the order they were submitted.
func (s *session) submit(msg Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("server is shutting down")
	}
	if len(s.queue) >= s.max {
		return errQueueFull
	}
	s.queue = append(s.queue, msg)
	s.cond.Signal()
	return nil
}

// cancel stops the job that is running now, and only that job. Jobs still
// queued are untouched and keep their order. It reports whether a running job
// accepted the cancel.
func (s *session) cancel() bool {
	s.mu.Lock()
	j := s.current
	s.mu.Unlock()
	if j == nil {
		return false
	}

	s.emitMu.Lock()
	// The job may have finished between the two locks. Its final status is
	// already out (or about to be, under emitMu); never touch the next job.
	if j.finalized || j.cancelRequested {
		s.emitMu.Unlock()
		return false
	}
	// From here on, emitProgress drops this job's progress. Release the lock
	// before cancelling so a slow client write cannot delay the kill.
	j.cancelRequested = true
	s.emitMu.Unlock()

	j.cancel()
	return true
}

// close stops the worker once the job it is running returns. Pending jobs are
// not started.
func (s *session) close() {
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// wait blocks until the worker has stopped.
func (s *session) wait() { <-s.done }

func (s *session) worker() {
	defer close(s.done)
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}

		// Dequeue and become the current job atomically. A cancel that arrives
		// from here on reaches this job, even if its encoder has not started a
		// process yet.
		msg := s.queue[0]
		s.queue = s.queue[1:]
		ctx, cancel := context.WithCancel(context.Background())
		j := &job{id: s.nextID, msg: msg, ctx: ctx, cancel: cancel}
		s.nextID++
		s.current = j
		s.mu.Unlock()

		s.runJob(j)

		cancel()
		s.mu.Lock()
		s.current = nil
		s.mu.Unlock()
	}
}

func (s *session) runJob(j *job) {
	err := s.encode(j.ctx, j.msg, func(st Status) { s.emitProgress(j, st) })

	switch {
	case errors.Is(err, ffmpeg.ErrCancelled):
		s.finalize(j, Status{JobID: j.id, Cancelled: true})
	case err != nil:
		s.finalize(j, Status{JobID: j.id, Err: err.Error()})
	default:
		s.finalize(j, Status{JobID: j.id, Percent: 100})
	}
}

// emitProgress forwards progress for the job that produced it. Progress from a
// job that has been finalized or cancelled is dropped, so a late tick from one
// task can never show up in another task's status stream.
func (s *session) emitProgress(j *job, st Status) {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	if j.finalized || j.cancelRequested {
		return
	}
	st.JobID = j.id
	s.broadcast(st)
}

// finalize sends a job's one and only final status. Later calls, and any
// progress that races with it, are ignored.
func (s *session) finalize(j *job, st Status) {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	if j.finalized {
		return
	}
	j.finalized = true
	s.broadcast(st)
}
