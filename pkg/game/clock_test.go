package game

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sauerbraten/waiter/pkg/protocol/nmc"
	"github.com/sauerbraten/waiter/pkg/protocol/playerstate"
)

type recordedBroadcast struct {
	typ  nmc.ID
	args []any
}

// fakeServer implements the Server interface the game package expects and
// records everything the clock broadcasts.
type fakeServer struct {
	mu            sync.Mutex
	players       []*Player
	broadcasts    []recordedBroadcast
	intermissions int
}

func newFakeServer(players ...*Player) *fakeServer {
	return &fakeServer{players: players}
}

func (f *fakeServer) GameDuration() time.Duration { return time.Hour }

func (f *fakeServer) Broadcast(typ nmc.ID, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.broadcasts = append(f.broadcasts, recordedBroadcast{typ, args})
}

func (f *fakeServer) Intermission() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intermissions++
}

func (f *fakeServer) ForEachPlayer(do func(*Player)) {
	for _, p := range f.players {
		do(p)
	}
}

func (f *fakeServer) UniqueName(p *Player) string { return fmt.Sprintf("player-%d", p.CN) }

func (f *fakeServer) NumberOfPlayers() (n int) {
	for _, p := range f.players {
		if p.State != playerstate.Spectator {
			n++
		}
	}
	return
}

// countPauseGame returns the number of PauseGame broadcasts with pause=1 and
// pause=0, respectively.
func (f *fakeServer) countPauseGame() (pauses, resumes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range f.broadcasts {
		if b.typ != nmc.PauseGame || len(b.args) == 0 {
			continue
		}
		if b.args[0] == 1 {
			pauses++
		} else {
			resumes++
		}
	}
	return
}

func (f *fakeServer) countMessagesContaining(sub string) (n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range f.broadcasts {
		if b.typ != nmc.ServerMessage || len(b.args) == 0 {
			continue
		}
		if msg, ok := b.args[0].(string); ok && strings.Contains(msg, sub) {
			n++
		}
	}
	return
}

func (f *fakeServer) waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func shortCountdown(t *testing.T) {
	t.Helper()
	old := resumeCountdownStep
	resumeCountdownStep = 5 * time.Millisecond
	t.Cleanup(func() { resumeCountdownStep = old })
}

func playingPlayers(cns ...uint32) []*Player {
	players := []*Player{}
	for _, cn := range cns {
		p := NewPlayer(cn)
		p.State = playerstate.Dead
		players = append(players, &p)
	}
	return players
}

// A player leaving while the server waits for everyone to load the map must
// not block the game: the wait only applies to players still present.
func TestCompetitiveClockLeaveDuringSpawnWait(t *testing.T) {
	shortCountdown(t)
	players := playingPlayers(0, 1)
	fs := newFakeServer(players...)
	c := NewCompetitiveClock(fs, nil)
	defer c.Stop()

	c.Start()
	if !c.Paused() {
		t.Fatal("clock should be paused while waiting for spawns")
	}

	c.Leave(players[1])   // player 1 leaves before loading the map
	c.Spawned(players[0]) // player 0 finishes loading

	fs.waitFor(t, "game to resume after all remaining players spawned", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 1
	})

	pauses, resumes := fs.countPauseGame()
	if pauses != 1 || resumes != 1 {
		t.Fatalf("expected exactly 1 pause and 1 resume, got %d pauses and %d resumes", pauses, resumes)
	}
	if got := fs.countMessagesContaining("all players spawned"); got != 1 {
		t.Fatalf("expected 'all players spawned' message exactly once, got %d", got)
	}
}

// Spawns happening after the initial spawn wait (e.g. regular mid-game
// respawns) must not interfere with the clock. The old WaitGroup-based
// implementation panicked with a negative counter here.
func TestCompetitiveClockSpawnedAfterSpawnWait(t *testing.T) {
	shortCountdown(t)
	players := playingPlayers(0)
	fs := newFakeServer(players...)
	c := NewCompetitiveClock(fs, nil)
	defer c.Stop()

	c.Start()
	c.Spawned(players[0])
	fs.waitFor(t, "game to resume", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 1
	})

	// mid-game respawns
	c.Spawned(players[0])
	c.Spawned(players[0])

	if c.Paused() {
		t.Fatal("clock should not be paused after mid-game respawns")
	}
}

// A player leaving during the resume countdown aborts the countdown; the game
// stays paused and no stale countdown action fires afterwards.
func TestCompetitiveClockLeaveDuringResumeCountdown(t *testing.T) {
	shortCountdown(t)
	players := playingPlayers(0, 1)
	fs := newFakeServer(players...)
	c := NewCompetitiveClock(fs, nil)
	defer c.Stop()

	c.Start()
	c.Spawned(players[0])
	c.Spawned(players[1])
	fs.waitFor(t, "game to resume", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 1
	})

	c.Pause(nil) // admin pauses

	countdownMessagesBefore := fs.countMessagesContaining("...")
	c.Resume(nil)       // admin resumes: countdown starts
	c.Leave(players[1]) // player leaves during the countdown

	time.Sleep(20 * resumeCountdownStep) // way past the end of the countdown

	pauses, resumes := fs.countPauseGame()
	if pauses != 2 || resumes != 1 {
		t.Fatalf("expected 2 pauses and 1 resume, got %d pauses and %d resumes", pauses, resumes)
	}
	if !c.Paused() {
		t.Fatal("game should still be paused after the countdown was aborted")
	}
	if got := fs.countMessagesContaining("resuming aborted"); got != 1 {
		t.Fatalf("expected 'resuming aborted' exactly once, got %d", got)
	}
	if got := fs.countMessagesContaining("..."); got != countdownMessagesBefore {
		t.Fatalf("expected no stale countdown messages, got %d new ones", got-countdownMessagesBefore)
	}
}

// An automatic pause triggered by a player leaving must happen only once:
// after an admin manually resumed the game, a late or duplicate leave event
// for the same departure must not pause the game again. A different player
// leaving triggers a new auto-pause, though.
func TestCompetitiveClockLateLeaveDoesNotRepause(t *testing.T) {
	shortCountdown(t)
	players := playingPlayers(0, 1, 2)
	fs := newFakeServer(players...)
	c := NewCompetitiveClock(fs, nil)
	defer c.Stop()

	c.Start()
	for _, p := range players {
		c.Spawned(p)
	}
	fs.waitFor(t, "game to resume", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 1
	})

	c.Leave(players[2]) // player 2 leaves -> automatic pause
	if got := fs.countMessagesContaining("a player left the game"); got != 1 {
		t.Fatalf("expected 'a player left the game' exactly once, got %d", got)
	}

	c.Resume(nil) // admin resumes manually
	fs.waitFor(t, "game to resume", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 2
	})

	// a late, duplicate leave event for the same player must not pause again
	c.Leave(players[2])
	time.Sleep(20 * resumeCountdownStep)

	pauses, resumes := fs.countPauseGame()
	if pauses != 2 || resumes != 2 {
		t.Fatalf("late leave event changed the pause state: got %d pauses and %d resumes", pauses, resumes)
	}
	if got := fs.countMessagesContaining("a player left the game"); got != 1 {
		t.Fatalf("late leave event was announced again: got %d messages", got)
	}

	// a different player leaving triggers a new automatic pause
	c.Leave(players[1])
	pauses, _ = fs.countPauseGame()
	if pauses != 3 {
		t.Fatalf("expected a new automatic pause when another player leaves, got %d pauses", pauses)
	}
	if got := fs.countMessagesContaining("a player left the game"); got != 2 {
		t.Fatalf("expected 'a player left the game' twice, got %d", got)
	}
}

// A (re)joining player starts a new session: their next departure triggers an
// automatic pause again, even if a previous session with the same CN already
// caused one.
func TestCompetitiveClockRejoinStartsNewSession(t *testing.T) {
	shortCountdown(t)
	players := playingPlayers(0, 1)
	fs := newFakeServer(players...)
	c := NewCompetitiveClock(fs, nil)
	defer c.Stop()

	c.Start()
	for _, p := range players {
		c.Spawned(p)
	}
	fs.waitFor(t, "game to resume", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 1
	})

	c.Leave(players[1]) // auto-pause
	c.Resume(nil)       // admin resumes
	fs.waitFor(t, "game to resume", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 2
	})

	c.Join(players[1])  // same CN reconnects -> new session
	c.Leave(players[1]) // must auto-pause again

	if got := fs.countMessagesContaining("a player left the game"); got != 2 {
		t.Fatalf("expected a second auto-pause after rejoin, got %d 'a player left the game' messages", got)
	}
	pauses, _ := fs.countPauseGame()
	if pauses != 3 {
		t.Fatalf("expected 3 pauses, got %d", pauses)
	}
}

// Stopping the clock (map change) must clean up all pending actions: no stale
// countdown or spawn-wait action may fire afterwards, and a new clock starts
// from a clean state.
func TestCompetitiveClockStopCleansUpPendingActions(t *testing.T) {
	shortCountdown(t)
	players := playingPlayers(0, 1)
	fs := newFakeServer(players...)
	c := NewCompetitiveClock(fs, nil)

	c.Start()
	for _, p := range players {
		c.Spawned(p)
	}
	fs.waitFor(t, "game to resume", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 1
	})

	c.Pause(nil)
	countdownMessagesBefore := fs.countMessagesContaining("...")
	c.Resume(nil) // countdown pending...
	c.Stop()      // ...when the map change begins

	time.Sleep(20 * resumeCountdownStep)

	pauses, resumes := fs.countPauseGame()
	if pauses != 2 || resumes != 1 {
		t.Fatalf("stale actions fired after Stop: got %d pauses and %d resumes", pauses, resumes)
	}
	if got := fs.countMessagesContaining("..."); got != countdownMessagesBefore {
		t.Fatalf("countdown messages were broadcast after Stop: got %d new ones", got-countdownMessagesBefore)
	}

	// late spawn and leave events on the stopped clock are ignored
	c.Spawned(players[0])
	c.Leave(players[1])
	time.Sleep(20 * resumeCountdownStep)
	pauses, resumes = fs.countPauseGame()
	if pauses != 2 || resumes != 1 {
		t.Fatalf("events on stopped clock changed state: got %d pauses and %d resumes", pauses, resumes)
	}

	// a new clock (new map) starts from a clean state
	c2 := NewCompetitiveClock(fs, nil)
	defer c2.Stop()
	c2.Start()
	if !c2.Paused() {
		t.Fatal("new clock should be paused while waiting for spawns")
	}
	for _, p := range players {
		c2.Spawned(p)
	}
	fs.waitFor(t, "new game to resume", func() bool {
		_, resumes := fs.countPauseGame()
		return resumes == 2
	})
	pauses, resumes = fs.countPauseGame()
	if pauses != 3 || resumes != 2 {
		t.Fatalf("expected 3 pauses and 2 resumes in total, got %d and %d", pauses, resumes)
	}
}

// A spawn wait interrupted by Stop must not resume the old clock once the
// remaining players would have spawned.
func TestCompetitiveClockStopDuringSpawnWait(t *testing.T) {
	shortCountdown(t)
	players := playingPlayers(0, 1)
	fs := newFakeServer(players...)
	c := NewCompetitiveClock(fs, nil)

	c.Start()
	if !c.Paused() {
		t.Fatal("clock should be paused while waiting for spawns")
	}
	c.Stop() // map change before anyone spawned

	c.Spawned(players[0])
	c.Spawned(players[1])
	time.Sleep(20 * resumeCountdownStep)

	pauses, resumes := fs.countPauseGame()
	if pauses != 1 || resumes != 0 {
		t.Fatalf("stopped clock fired actions: got %d pauses and %d resumes", pauses, resumes)
	}
	if got := fs.countMessagesContaining("all players spawned"); got != 0 {
		t.Fatalf("stopped clock announced spawn wait completion %d times", got)
	}
}
