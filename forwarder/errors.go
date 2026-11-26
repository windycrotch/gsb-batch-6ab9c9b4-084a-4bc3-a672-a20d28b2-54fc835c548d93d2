package forwarder

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors describing forwarder lifecycle outcomes
var (
	// errForwarderStopping is returned / recorded once a forwarder has begun shutting
	// down; in-flight work observing this is reported as explicitly cancelled
	errForwarderStopping = errors.New("segment forwarder is stopping")
)

// interruptibleSleep waits for the given duration while remaining interruptible by
// context cancellation or forwarder shutdown
func interruptibleSleep(ctx context.Context, stopCh <-chan struct{}, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-stopCh:
		return errForwarderStopping
	}
}
