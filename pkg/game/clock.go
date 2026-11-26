package game

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sauerbraten/timer"
	"github.com/sauerbraten/waiter/pkg/protocol/nmc"
	"github.com/sauerbraten/waiter/pkg/protocol/playerstate"
)

// implemented by modes with modes with either flags or pickups
type hasTimers interface {
	// Start() // flags/pickups "auto-start" once they are received from a client
	Pause()
	Resume()
	Stop()
	CleanUp()
}

var (
	_ hasTimers = &flagMode{}
	_ hasTimers = &handlesPickups{}
)

type casualClock struct {
	s Server
	t *timer.Timer
	m hasTimers
	// stopped is true after Stop was called: a stopped clock must not be
	// paused, resumed or otherwise touched again (e.g. by late events
	// arriving after a map change)
	stopped atomic.Bool
}

func NewCasualClock(s Server, m Mode) *casualClock {
	timedMode, _ := m.(hasTimers) // nil if mode has no timers
	return &casualClock{
		s: s,
		t: timer.AfterFunc(s.GameDuration(), s.Intermission),
		m: timedMode,
	}
}

func (c *casualClock) Start() {
	c.stopped.Store(false)
	c.t.Start()
	log.Println("started game timer, time left:", c.t.TimeLeft())
	c.s.Broadcast(nmc.TimeLeft, int32(c.t.TimeLeft()/time.Second))
}

func (c *casualClock) Pause(p *Player) {
	if c.stopped.Load() || c.t.Paused() {
		return
	}
	cn := -1
	if p != nil {
		cn = int(p.CN)
	}
	c.s.Broadcast(nmc.PauseGame, 1, cn)
	c.t.Pause()
	if c.m != nil {
		c.m.Pause()
	}
}

func (c *casualClock) Paused() bool {
	return c.t.Paused()
}

func (c *casualClock) Resume(p *Player) {
	if c.stopped.Load() || !c.t.Paused() {
		return
	}
	cn := -1
	if p != nil {
		cn = int(p.CN)
	}
	c.s.Broadcast(nmc.PauseGame, 0, cn)
	c.t.Start()
	if c.m != nil {
		c.m.Resume()
	}
}

func (*casualClock) Join(*Player)      {}
func (*casualClock) Spawned(p *Player) {}
func (*casualClock) Leave(*Player)     {}

func (c *casualClock) Stop() {
	if c.stopped.Swap(true) {
		return
	}
	c.s.Broadcast(nmc.TimeLeft, 0)
	log.Println("stopping game timer, time left:", c.t.TimeLeft())
	c.t.Stop()
	if c.m != nil {
		c.m.Stop()
	}
}

func (c *casualClock) Ended() bool {
	return c.t.TimeLeft() <= 0
}

func (c *casualClock) TimeLeft() time.Duration {
	return c.t.TimeLeft()
}

func (c *casualClock) SetTimeLeft(d time.Duration) {
	if c.stopped.Load() {
		return
	}
	log.Println("setting game timer to", d)
	if !c.t.SetTimeLeft(d) {
		log.Println("game timer had already expired")
	}
	c.s.Broadcast(nmc.TimeLeft, int32(d/time.Second))
}

// resumeCountdownStep is the delay between the countdown messages broadcast
// before a competitive game is resumed. It is a variable so tests can use a
// shorter countdown.
var resumeCountdownStep = 650 * time.Millisecond

// A competitiveClock pauses the game until all players have loaded the map and
// automatically pauses the game when a player leaves. It serializes all state
// changes through a mutex and uses a generation counter to invalidate pending
// goroutines and timers when the game is stopped or (re)started, so at any
// moment there is at most one pending pause, resume or spawn-wait action and
// no stale action can fire after a map change.
type competitiveClock struct {
	*casualClock

	mtx              sync.Mutex
	running          bool                // true between Start and Stop; late events on a stopped clock are ignored
	generation       uint64              // bumped on Start and Stop to invalidate pending goroutines and timers
	wake             chan struct{}       // wakes the spawn-waiting goroutine of the current generation
	waitingForSpawns bool                // true while waiting for players to spawn after a map change
	pendingSpawns    map[uint32]struct{} // CNs of players we are still waiting for
	autoPaused       map[uint32]struct{} // CNs of players whose departure already triggered an automatic pause
	resumeCountdown  []*time.Timer       // pending countdown actions; at most one countdown at a time
}

func NewCompetitiveClock(s Server, m Mode) *competitiveClock {
	return &competitiveClock{
		casualClock: NewCasualClock(s, m),
		wake:        make(chan struct{}, 1),
	}
}

func (c *competitiveClock) Start() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.resetLocked()
	c.wake = make(chan struct{}, 1)
	c.pendingSpawns = map[uint32]struct{}{}
	c.autoPaused = map[uint32]struct{}{}
	c.running = true

	c.casualClock.Start()

	c.s.ForEachPlayer(func(p *Player) {
		if p.State != playerstate.Spectator {
			c.pendingSpawns[p.CN] = struct{}{}
		}
	})
	if len(c.pendingSpawns) > 0 {
		c.waitingForSpawns = true
		c.s.Broadcast(nmc.ServerMessage, "waiting for all players to load the map")
		c.pauseLocked(nil)
		go c.awaitSpawns(c.generation, c.wake)
	}
}

// awaitSpawns waits until all pending players spawned (or left), then resumes
// the game. It exits without doing anything when the clock was stopped or
// restarted in the meantime.
func (c *competitiveClock) awaitSpawns(gen uint64, wake chan struct{}) {
	for {
		c.mtx.Lock()
		if gen != c.generation || !c.waitingForSpawns {
			c.mtx.Unlock()
			return
		}
		if len(c.pendingSpawns) == 0 {
			c.waitingForSpawns = false
			c.mtx.Unlock()
			c.s.Broadcast(nmc.ServerMessage, "all players spawned, starting game")
			c.Resume(nil)
			return
		}
		c.mtx.Unlock()
		<-wake
	}
}

// resetLocked invalidates all pending actions and clears all per-game state.
// The caller must hold c.mtx.
func (c *competitiveClock) resetLocked() {
	c.running = false
	c.generation++
	c.cancelCountdownLocked()
	c.waitingForSpawns = false
	c.pendingSpawns = nil
	c.autoPaused = nil
	c.notifyLocked() // wake the spawn-waiting goroutine of the previous generation, if any
}

// notifyLocked wakes the spawn-waiting goroutine of the current generation.
// The caller must hold c.mtx.
func (c *competitiveClock) notifyLocked() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// cancelCountdownLocked stops the pending resume countdown, if any, and
// reports whether there was one. The caller must hold c.mtx.
func (c *competitiveClock) cancelCountdownLocked() bool {
	if len(c.resumeCountdown) == 0 {
		return false
	}
	for _, action := range c.resumeCountdown {
		if action != nil {
			action.Stop()
		}
	}
	c.resumeCountdown = nil
	return true
}

func (c *competitiveClock) Join(p *Player) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	// a (re)joining player starts a new session: any automatic pause a
	// previous session of this CN triggered must not suppress future ones
	delete(c.autoPaused, p.CN)

	if p.State != playerstate.Spectator && c.waitingForSpawns {
		c.pendingSpawns[p.CN] = struct{}{}
	}
}

func (c *competitiveClock) Spawned(p *Player) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if !c.waitingForSpawns {
		return
	}
	delete(c.pendingSpawns, p.CN)
	c.notifyLocked()
}

func (c *competitiveClock) Pause(p *Player) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if !c.running {
		return
	}
	c.pauseLocked(p)
}

// The caller must hold c.mtx.
func (c *competitiveClock) pauseLocked(p *Player) {
	if !c.Paused() {
		c.casualClock.Pause(p)
		return
	}
	// the game is already paused: a pause request aborts a pending resume
	if c.cancelCountdownLocked() {
		c.s.Broadcast(nmc.ServerMessage, "resuming aborted")
	}
}

func (c *competitiveClock) Resume(p *Player) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if !c.running {
		return
	}

	if c.cancelCountdownLocked() {
		// a resume was pending, cancel it
		c.s.Broadcast(nmc.ServerMessage, "resuming aborted")
		return
	}

	if !c.Paused() {
		// the game is not paused, there is nothing to resume
		return
	}

	if p != nil {
		c.s.Broadcast(nmc.ServerMessage, fmt.Sprintf("%s wants to resume the game", c.s.UniqueName(p)))
	}
	c.s.Broadcast(nmc.ServerMessage, "resuming game in 3")

	cn := -1
	if p != nil {
		cn = int(p.CN)
	}
	gen := c.generation
	c.resumeCountdown = []*time.Timer{
		time.AfterFunc(1*resumeCountdownStep, c.countdownMessage(gen, "... 2")),
		time.AfterFunc(2*resumeCountdownStep, c.countdownMessage(gen, "... 1")),
		time.AfterFunc(3*resumeCountdownStep, func() { c.finishResume(gen, cn) }),
	}
}

// countdownMessage broadcasts a countdown message unless the countdown was
// cancelled or the clock was stopped or restarted in the meantime.
func (c *competitiveClock) countdownMessage(gen uint64, msg string) func() {
	return func() {
		c.mtx.Lock()
		defer c.mtx.Unlock()
		if gen != c.generation || len(c.resumeCountdown) == 0 {
			return
		}
		c.s.Broadcast(nmc.ServerMessage, msg)
	}
}

// finishResume resumes the game unless the countdown was cancelled or the
// clock was stopped or restarted in the meantime.
func (c *competitiveClock) finishResume(gen uint64, cn int) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if gen != c.generation || len(c.resumeCountdown) == 0 {
		return
	}
	c.resumeCountdown = nil
	c.s.Broadcast(nmc.PauseGame, 0, cn)
	c.t.Start()
	if c.m != nil {
		c.m.Resume()
	}
}

func (c *competitiveClock) Leave(p *Player) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if !c.running {
		return
	}

	if c.waitingForSpawns {
		// don't wait for players who are not there anymore
		delete(c.pendingSpawns, p.CN)
		c.notifyLocked()
	}

	if p.State == playerstate.Spectator || c.Ended() {
		return
	}

	if _, ok := c.autoPaused[p.CN]; ok {
		// this departure already triggered an automatic pause (e.g. a late or
		// duplicate leave event): don't pause again
		return
	}
	c.autoPaused[p.CN] = struct{}{}

	c.s.Broadcast(nmc.ServerMessage, "a player left the game")
	c.pauseLocked(nil)
}

func (c *competitiveClock) Stop() {
	c.mtx.Lock()
	c.resetLocked()
	c.mtx.Unlock()
	c.casualClock.Stop()
}

func (c *competitiveClock) ToCasual() *casualClock { return c.casualClock }
