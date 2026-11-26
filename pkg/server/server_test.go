package server

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sauerbraten/waiter/pkg/bans"
	"github.com/sauerbraten/waiter/pkg/enet"
	"github.com/sauerbraten/waiter/pkg/maprot"
	"github.com/sauerbraten/waiter/pkg/protocol"
	"github.com/sauerbraten/waiter/pkg/protocol/disconnectreason"
	"github.com/sauerbraten/waiter/pkg/protocol/gamemode"
	"github.com/sauerbraten/waiter/pkg/protocol/nmc"
	"github.com/sauerbraten/waiter/pkg/protocol/playerstate"
	"github.com/sauerbraten/waiter/pkg/protocol/role"
)

// testPeer is an enet.Peer (using the non-CGO stub) that records every packet
// the server sends to it.
type testPeer struct {
	peer     *enet.Peer
	mu       sync.Mutex
	received [][]byte
}

func newTestPeer(port int) *testPeer {
	tp := &testPeer{}
	tp.peer = &enet.Peer{
		Address: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port},
	}
	tp.peer.SendHook = func(channel uint8, payload []byte) {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		tp.mu.Lock()
		defer tp.mu.Unlock()
		tp.received = append(tp.received, cp)
	}
	return tp
}

func (tp *testPeer) numReceived() int {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return len(tp.received)
}

// countPauseGame counts PauseGame packets with the given pause value (0 or 1)
// sent to this peer.
func (tp *testPeer) countPauseGame(pause int32) (n int) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	for _, pkt := range tp.received {
		p := protocol.Packet(pkt)
		typ, ok := p.GetInt()
		if !ok || nmc.ID(typ) != nmc.PauseGame {
			continue
		}
		val, ok := p.GetInt()
		if ok && val == pause {
			n++
		}
	}
	return
}

// countMessagesContaining counts ServerMessage packets containing sub sent to
// this peer.
func (tp *testPeer) countMessagesContaining(sub string) (n int) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	for _, pkt := range tp.received {
		p := protocol.Packet(pkt)
		typ, ok := p.GetInt()
		if !ok || nmc.ID(typ) != nmc.ServerMessage {
			continue
		}
		msg, ok := p.GetString()
		if ok && strings.Contains(msg, sub) {
			n++
		}
	}
	return
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	conf := &Config{
		_Config: _Config{
			FallbackGameModeID: gamemode.FFA,
			MaxClients:         16,
			MapPools: maprot.Pools{
				Deathmatch: []string{"dm1", "dm2"},
				CTF:        []string{"ctf1"},
				Capture:    []string{"cap1"},
			},
		},
		GameDuration: 10 * time.Minute,
	}
	host, err := enet.NewHost("", 0)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := New(host, conf, bans.New())
	s.CompetitiveMode = true
	s.Empty()
	return s
}

func joinTestClient(t *testing.T, s *Server, name string, port int) (*Client, *testPeer) {
	t.Helper()
	tp := newTestPeer(port)
	s.Connect(tp.peer)
	c := s.Clients.GetClientByPeer(tp.peer)
	if c == nil {
		t.Fatal("client not found after connect")
	}
	s.TryJoin(c, name, 0, "", "")
	if !c.Joined {
		t.Fatalf("client '%s' did not join", name)
	}
	return c, tp
}

func confirmSpawn(t *testing.T, s *Server, c *Client) {
	t.Helper()
	s.ConfirmSpawn(c, c.LifeSequence, 0)
	if c.State != playerstate.Alive {
		t.Fatalf("client '%s' did not spawn", c.Name)
	}
}

// TestCompetitiveModeInterleavedEvents drives the server through interleaved
// leave, pause, resume and map change events in competitive mode and asserts
// that the pause and map state broadcast to the players stays self-consistent
// and that departed players don't receive any more messages.
func TestCompetitiveModeInterleavedEvents(t *testing.T) {
	s := newTestServer(t)

	admin, adminPeer := joinTestClient(t, s, "admin", 4001)
	admin.Role = role.Master // keep the server "supervised" so it stays in competitive mode
	p2, p2Peer := joinTestClient(t, s, "p2", 4002)
	p3, p3Peer := joinTestClient(t, s, "p3", 4003)
	p2SessionID := p2.SessionID

	// start a competitive map: the server pauses and waits for everyone to load
	s.StartGame(s.StartMode(gamemode.FFA), "dm1")
	for name, tp := range map[string]*testPeer{"admin": adminPeer, "p2": p2Peer, "p3": p3Peer} {
		if got := tp.countPauseGame(1); got != 1 {
			t.Fatalf("%s: expected 1 pause broadcast at map start, got %d", name, got)
		}
		if got := tp.countMessagesContaining("waiting for all players"); got != 1 {
			t.Fatalf("%s: expected 'waiting for all players' message, got %d", name, got)
		}
	}

	// two players finish loading; the third one leaves before spawning
	confirmSpawn(t, s, admin)
	confirmSpawn(t, s, p2)
	p3sent := p3Peer.numReceived()
	s.Disconnect(p3, disconnectreason.None)

	// the game must resume for the remaining players (the wait only applies
	// to players still present)
	waitFor(t, "game to resume after all remaining players spawned", func() bool {
		return adminPeer.countPauseGame(0) == 1 && p2Peer.countPauseGame(0) == 1
	})

	// the departed player must not receive any more messages
	time.Sleep(100 * time.Millisecond)
	if got := p3Peer.numReceived(); got != p3sent {
		t.Fatalf("departed player received %d packets after disconnect", got-p3sent)
	}

	// p2 leaves mid-game: the game auto-pauses, only for the remaining player
	leftMessagesBefore := adminPeer.countMessagesContaining("a player left the game")
	p2sent := p2Peer.numReceived()
	s.Disconnect(p2, disconnectreason.None)
	if got := adminPeer.countPauseGame(1); got != 2 {
		t.Fatalf("expected auto-pause after player left, admin got %d pauses", got)
	}
	if got := adminPeer.countMessagesContaining("a player left the game"); got != leftMessagesBefore+1 {
		t.Fatalf("expected the leave to be announced once, got %d new messages", got-leftMessagesBefore)
	}
	time.Sleep(100 * time.Millisecond)
	if got := p2Peer.numReceived(); got != p2sent {
		t.Fatalf("departed player received %d packets after disconnect", got-p2sent)
	}

	// the admin resumes the game manually
	s.Clock.Resume(&admin.Player)
	waitFor(t, "resume countdown to finish", func() bool {
		return adminPeer.countPauseGame(0) == 2
	})

	// a late leave event for p2 (already gone) must not pause the game again
	s.Clock.Leave(&p2.Player)
	s.Disconnect(p2, disconnectreason.None) // duplicate disconnect event
	time.Sleep(100 * time.Millisecond)
	if got := adminPeer.countPauseGame(1); got != 2 {
		t.Fatalf("late leave event paused the game again, admin got %d pauses", got)
	}
	if got := adminPeer.countMessagesContaining("a player left the game"); got != leftMessagesBefore+1 {
		t.Fatalf("late leave event was announced again, got %d new messages", got-leftMessagesBefore-1)
	}

	// pause, start a resume countdown, then change the map in the middle of
	// it: the old countdown must be cleaned up and never fire
	s.Clock.Pause(&admin.Player)
	if got := adminPeer.countPauseGame(1); got != 3 {
		t.Fatalf("expected manual pause, admin got %d pauses", got)
	}
	resumesBefore := adminPeer.countPauseGame(0)
	countdownMessagesBefore := adminPeer.countMessagesContaining("...")
	s.Clock.Resume(&admin.Player)                 // countdown starts...
	s.StartGame(s.StartMode(gamemode.FFA), "dm2") // ...but the map changes first

	// the new map pauses and waits for players to load again
	if got := adminPeer.countPauseGame(1); got != 4 {
		t.Fatalf("expected pause at new map start, admin got %d pauses", got)
	}

	// wait longer than the full resume countdown: nothing from the old
	// countdown may arrive
	time.Sleep(2500 * time.Millisecond)
	if got := adminPeer.countPauseGame(0); got != resumesBefore {
		t.Fatalf("stale countdown resumed the game after map change (%d resumes, want %d)", got, resumesBefore)
	}
	if got := adminPeer.countMessagesContaining("..."); got != countdownMessagesBefore {
		t.Fatalf("stale countdown sent %d messages after map change", got-countdownMessagesBefore)
	}

	// a reconnecting player is a new session: fresh identity and stats
	p2new, p2newPeer := joinTestClient(t, s, "p2-returning", 4004)
	if p2new.SessionID == p2SessionID {
		t.Fatal("reconnected player kept their old session ID")
	}
	if p2new.Frags != 0 || p2new.Deaths != 0 || p2new.Flags != 0 {
		t.Fatalf("reconnected player kept old stats: frags=%d deaths=%d flags=%d", p2new.Frags, p2new.Deaths, p2new.Flags)
	}

	// the reconnected player leaves while the server waits for everyone to
	// load the map; once the admin finishes loading, the game resumes
	p2newSent := p2newPeer.numReceived()
	s.Disconnect(p2new, disconnectreason.None)
	confirmSpawn(t, s, admin)
	waitFor(t, "game to resume after all remaining players spawned", func() bool {
		return adminPeer.countPauseGame(0) == resumesBefore+1
	})
	time.Sleep(100 * time.Millisecond)
	if got := p2newPeer.numReceived(); got != p2newSent {
		t.Fatalf("departed player received %d packets after disconnect", got-p2newSent)
	}

	// a client who connects but never joins must not affect the game when
	// they disconnect
	ghost := newTestPeer(4005)
	s.Connect(ghost.peer)
	ghostClient := s.Clients.GetClientByPeer(ghost.peer)
	pausesBefore := adminPeer.countPauseGame(1)
	leftMessagesBefore = adminPeer.countMessagesContaining("a player left the game")
	s.Disconnect(ghostClient, disconnectreason.None)
	time.Sleep(100 * time.Millisecond)
	if got := adminPeer.countPauseGame(1); got != pausesBefore {
		t.Fatalf("disconnect of unjoined client paused the game (%d pauses, want %d)", got, pausesBefore)
	}
	if got := adminPeer.countMessagesContaining("a player left the game"); got != leftMessagesBefore {
		t.Fatalf("disconnect of unjoined client was announced as a player leaving")
	}
}

// TestIntermissionKeepsSinglePendingMapChange makes sure repeated
// intermissions (e.g. triggered by a stale timer) never schedule more than
// one pending map change.
func TestIntermissionKeepsSinglePendingMapChange(t *testing.T) {
	s := newTestServer(t)

	s.Intermission()
	first := s.PendingMapChange
	if first == nil {
		t.Fatal("expected a pending map change after intermission")
	}
	s.Intermission()
	second := s.PendingMapChange
	if second == nil {
		t.Fatal("expected a pending map change after second intermission")
	}
	if first == second {
		t.Fatal("expected the second intermission to replace the pending map change")
	}

	// the map change fires only once, on the latest timer
	s.StartGame(s.StartMode(gamemode.FFA), "dm1")
	if s.PendingMapChange != nil {
		t.Fatal("expected no pending map change after the game started")
	}
}
