package vlc

import (
	"runtime"
	"sync"
)

// sessionEpoch identifies a single active media session of a Player.
//
// Every time the player media is replaced (Player.SetMedia,
// Player.LoadMediaFromPath, Player.LoadMediaFromURL or
// Player.LoadMediaFromReadSeeker) the epoch is rotated. Event subscriptions
// created through the player's EventManager are stamped with the epoch that
// was current when Attach was called. When the epoch changes, late events
// belonging to the previous media session are rejected by the event gate and
// never reach user code.
//
// The zero value is not usable; epochs are minted by an eventGate.
type sessionEpoch uint64

// initialSessionEpoch is the epoch of a gate before any media has been set.
// Subscriptions stamped with this epoch are player-lifetime subscriptions and
// survive media changes, which keeps the common "attach once before loading"
// usage working. Loading the first media opens epoch 1 and stamps all later
// attachments with a media scoped epoch.
const initialSessionEpoch sessionEpoch = 0

// eventGate is the concurrency safe gate through which every eventDispatch
// invocation passes before entering user callback code.
//
// It provides three guarantees:
//
//   - Stale session rejection: a dispatch stamped with an epoch that is no
//     longer current is dropped. This is what prevents an old media event from
//     reaching user code after a media switch.
//   - Release rejection: once close is called, all dispatches are dropped.
//   - In-flight tracking: every accepted dispatch is tracked with a
//     sync.WaitGroup so Release can drain callbacks that are already running
//     before releasing the underlying native player.
//
// eventGate additionally records the goroutines currently executing inside a
// callback so Release can detect that it was called from within an event
// callback and avoid self-deadlocking on wait.
type eventGate struct {
	mu sync.Mutex

	// epoch is the currently active session generation.
	epoch sessionEpoch

	// closed is set by close. Once closed, enter always fails.
	closed bool

	// active tracks accepted, not yet finished dispatches.
	active sync.WaitGroup

	// callbackGoroutines counts dispatch nesting per goroutine ID.
	callbackGoroutines map[int64]int

	// ids tracks event subscriptions created through a scoped EventManager so
	// Player.Release can remove their Go contexts from the event registry.
	ids map[EventID]struct{}
}

func newEventGate() *eventGate {
	return &eventGate{
		epoch:              initialSessionEpoch,
		callbackGoroutines: make(map[int64]int),
		ids:                make(map[EventID]struct{}),
	}
}

// stamp returns the epoch to associate with a new subscription.
func (g *eventGate) stamp() sessionEpoch {
	if g == nil {
		return initialSessionEpoch
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.epoch
}

// rotate opens a new media session. Subscriptions stamped with an older,
// non-initial epoch become stale and are rejected by enter. Subscriptions
// stamped with initialSessionEpoch are player-lifetime subscriptions and keep
// being delivered until the gate is closed.
func (g *eventGate) rotate() sessionEpoch {
	if g == nil {
		return initialSessionEpoch
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.epoch++
	}
	return g.epoch
}

// close permanently shuts the gate. It does not wait for in-flight dispatches;
// call wait after close to drain them.
func (g *eventGate) close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closed = true
	g.epoch++
	g.mu.Unlock()
}

// enter validates a dispatch stamped with epoch and, when accepted, records
// the current goroutine as executing user callback code. The returned done
// function must be deferred by the caller. enter returns false when the event
// is stale or the gate is closed, in which case done is nil.
func (g *eventGate) enter(epoch sessionEpoch) (done func(), ok bool) {
	if g == nil {
		// No gate means no filtering (e.g. media/list player event managers).
		return func() {}, true
	}

	gid := currentGoroutineID()
	g.mu.Lock()
	if g.closed || !g.allowedLocked(epoch) {
		g.mu.Unlock()
		return nil, false
	}
	g.active.Add(1)
	g.callbackGoroutines[gid]++
	g.mu.Unlock()

	released := false
	done = func() {
		if released {
			return
		}
		released = true

		g.mu.Lock()
		if depth := g.callbackGoroutines[gid]; depth <= 1 {
			delete(g.callbackGoroutines, gid)
		} else {
			g.callbackGoroutines[gid] = depth - 1
		}
		g.mu.Unlock()

		g.active.Done()
	}
	return done, true
}

// allowedLocked reports whether a subscription with epoch may be dispatched.
// The initial epoch denotes a player-lifetime subscription which survives
// rotations but is still rejected once the gate is closed.
func (g *eventGate) allowedLocked(epoch sessionEpoch) bool {
	if epoch == initialSessionEpoch {
		return true
	}
	return epoch == g.epoch
}

// wait blocks until every accepted dispatch has finished. It must not be
// called from a goroutine currently inside one of the gate's callbacks; use
// inCallback to defer teardown in that case.
func (g *eventGate) wait() {
	if g == nil {
		return
	}
	g.active.Wait()
}

// inCallback reports whether the calling goroutine is currently executing
// inside an accepted dispatch of this gate.
func (g *eventGate) inCallback() bool {
	if g == nil {
		return false
	}
	gid := currentGoroutineID()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.callbackGoroutines[gid] > 0
}

// track records a scoped event subscription so it can be removed on release.
func (g *eventGate) track(id EventID) {
	if g == nil || id == 0 {
		return
	}
	g.mu.Lock()
	g.ids[id] = struct{}{}
	g.mu.Unlock()
}

// untrack removes a subscription record. It is called on Detach.
func (g *eventGate) untrack(id EventID) {
	if g == nil || id == 0 {
		return
	}
	g.mu.Lock()
	delete(g.ids, id)
	g.mu.Unlock()
}

// trackedIDs returns a snapshot of the tracked subscription IDs.
func (g *eventGate) trackedIDs() []EventID {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	ids := make([]EventID, 0, len(g.ids))
	for id := range g.ids {
		ids = append(ids, id)
	}
	g.mu.Unlock()
	return ids
}

// currentGoroutineID returns the runtime ID of the calling goroutine.
//
// It is used only as a re-entrancy hint so Release can avoid waiting on a
// WaitGroup from within the very callback it would wait for. The ID is parsed
// from the stable first line of runtime.Stack ("goroutine N [").
func currentGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)

	const prefix = "goroutine "
	b := buf[len(prefix):n]

	var id int64
	for _, c := range b {
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + int64(c-'0')
	}
	return id
}

// playerSettings stores player level audio preferences that must survive
// media switches. libVLC keeps most of these on the player already, but the
// cached values allow the binding to re-apply them deterministically after a
// media change and to report the last explicit value even before an audio
// output becomes available.
type playerSettings struct {
	mu sync.RWMutex

	volume    int
	volumeSet bool

	muted   bool
	muteSet bool
}

// setVolume records the last explicitly requested volume.
func (s *playerSettings) setVolume(volume int) {
	s.mu.Lock()
	s.volume = volume
	s.volumeSet = true
	s.mu.Unlock()
}

// volumeValue returns the recorded volume, if any.
func (s *playerSettings) volumeValue() (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.volume, s.volumeSet
}

// setMuted records the last explicitly requested mute state.
func (s *playerSettings) setMuted(muted bool) {
	s.mu.Lock()
	s.muted = muted
	s.muteSet = true
	s.mu.Unlock()
}

// mutedValue returns the recorded mute state, if any.
func (s *playerSettings) mutedValue() (bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.muted, s.muteSet
}
