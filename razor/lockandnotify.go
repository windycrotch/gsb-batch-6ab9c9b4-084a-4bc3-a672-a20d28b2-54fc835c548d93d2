package razor

import (
	"sync"
	"time"
)

// LockAndNotify is a mutex paired with a coalescing wake-up signal.
//
// A notification raised while nobody is waiting is retained (at most one), so a
// waiter which registers slightly late still wakes up immediately. This matters
// because otherwise work queued just before the loop goes to sleep would sit
// idle until the loop's periodic timeout, making state changes very laggy.
type LockAndNotify struct {
	mutex sync.Mutex
	wake  chan struct{}
}

func NewLockAndNotify() *LockAndNotify {
	return &LockAndNotify{
		wake: make(chan struct{}, 1),
	}
}

func (l *LockAndNotify) Enter() {
	l.mutex.Lock()
}

func (l *LockAndNotify) Leave() {
	l.mutex.Unlock()
}

// Notify wakes a waiter, or leaves a pending wake-up for the next waiter.
// It never blocks.
func (l *LockAndNotify) Notify() {
	select {
	case l.wake <- struct{}{}:
	default:
		// A wake-up is already pending, which is sufficient.
	}
}

// Wait blocks until Notify is called or the delay elapses.
func (l *LockAndNotify) Wait(delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-l.wake:
	case <-timer.C:
	}
}
