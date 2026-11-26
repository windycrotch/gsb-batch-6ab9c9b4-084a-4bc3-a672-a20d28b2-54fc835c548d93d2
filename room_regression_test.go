package adler

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/lxzan/gws"
)

func TestRoomJoinRejectsClosureWhileWaitingForLock(t *testing.T) {
	a := New()
	r := a.NewRoom("lobby")
	s := &Session{adler: a}
	result := make(chan error, 1)

	r.mu.Lock()
	go func() { result <- r.Join(s) }()

	// Holding the session lock means Join passed its first closed check.
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for s.mu.TryLock() {
		s.mu.Unlock()
		select {
		case <-timer.C:
			r.mu.Unlock()
			t.Fatal("join did not acquire the session lock")
		default:
			runtime.Gosched()
		}
	}
	r.CloseRoom()
	r.mu.Unlock()

	select {
	case err := <-result:
		if !errors.Is(err, ErrRoomClosed) {
			t.Fatalf("join closed room: got=%v want=%v", err, ErrRoomClosed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("join deadlocked after the room closed")
	}

	if !s.mu.TryLock() {
		t.Fatal("rejected join left the session locked")
	}
	s.mu.Unlock()
	if s.Room() != nil || r.Len() != 0 {
		t.Fatal("rejected join changed room membership")
	}
}

func TestRoomCleanupConcurrentWithJoin(t *testing.T) {
	a := New()
	for range 1000 {
		r := a.NewRoom("lobby")
		s := &Session{adler: a}
		start := make(chan struct{})
		cleaned := make(chan struct{})
		joined := make(chan error, 1)

		go func() {
			<-start
			a.removeRoomIfEmpty(r)
			close(cleaned)
		}()
		go func() {
			<-start
			joined <- r.Join(s)
		}()
		close(start)

		var joinErr error
		select {
		case joinErr = <-joined:
		case <-time.After(2 * time.Second):
			t.Fatal("join deadlocked during room cleanup")
		}
		select {
		case <-cleaned:
		case <-time.After(2 * time.Second):
			t.Fatal("room cleanup deadlocked during join")
		}

		registered, lookupErr := a.Room("lobby")
		if joinErr == nil {
			if lookupErr != nil || registered != r || r.isClosed() || r.Len() != 1 || s.Room() != r {
				t.Fatal("cleanup removed or closed a room after a successful join")
			}
			r.Leave(s)
			continue
		}
		if !errors.Is(joinErr, ErrRoomClosed) {
			t.Fatalf("unexpected join error: %v", joinErr)
		}
		if !errors.Is(lookupErr, ErrRoomNotFound) || !r.isClosed() || r.Len() != 0 || s.Room() != nil {
			t.Fatal("deleted room retained membership or remained registered")
		}
	}
}

func TestRoomBroadcastBinaryOtherPreservesOpcode(t *testing.T) {
	a := New()
	r := a.NewRoom("lobby")
	sender := &Session{adler: a, output: make(chan message, 1)}
	receiver := &Session{adler: a, output: make(chan message, 1)}
	for _, s := range []*Session{sender, receiver} {
		if err := r.Join(s); err != nil {
			t.Fatalf("join room: %v", err)
		}
	}

	payload := []byte{0xff, 0x00, 0x80}
	r.BroadcastBinaryOther(payload, sender)
	select {
	case msg := <-receiver.output:
		if msg.messageType != gws.OpcodeBinary || string(msg.content) != string(payload) {
			t.Fatalf("unexpected broadcast: opcode=%v payload=%v", msg.messageType, msg.content)
		}
	default:
		t.Fatal("receiver did not get the binary broadcast")
	}
	select {
	case <-sender.output:
		t.Fatal("sender received its own broadcast")
	default:
	}
}
