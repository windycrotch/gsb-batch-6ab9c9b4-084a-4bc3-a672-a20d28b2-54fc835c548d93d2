package matchmaking

import (
	"errors"
	"testing"
	"time"

	"github.com/catalinfl/adler"
)

func TestRemoveFromQueueRejectsFullCommandBuffer(t *testing.T) {
	m := &Matchmaker{commands: make(chan matchmakingCommand, 1)}
	s := &adler.Session{}
	m.commands <- matchmakingCommand{kind: matchmakingCommandAdd, session: s}
	result := make(chan error, 1)
	go func() { result <- m.RemoveFromQueue(s) }()

	select {
	case err := <-result:
		if !errors.Is(err, ErrMatchmakerBusy) {
			t.Fatalf("full command buffer: got=%v want=%v", err, ErrMatchmakerBusy)
		}
	case <-time.After(time.Second):
		<-m.commands
		<-result
		t.Fatal("RemoveFromQueue blocked on a full command buffer")
	}
	if cmd := <-m.commands; cmd.kind != matchmakingCommandAdd {
		t.Fatal("rejected removal replaced the pending add command")
	}
	if err := m.RemoveFromQueue(s); err != nil {
		t.Fatalf("enqueue removal after freeing buffer: %v", err)
	}
	if cmd := <-m.commands; cmd.kind != matchmakingCommandRemove || cmd.session != s {
		t.Fatalf("unexpected removal command: %#v", cmd)
	}
}

func TestRemoveFromQueueRejectsNilSession(t *testing.T) {
	m := &Matchmaker{commands: make(chan matchmakingCommand, 1)}
	if err := m.RemoveFromQueue(nil); !errors.Is(err, ErrSessionNil) {
		t.Fatalf("nil session: got=%v want=%v", err, ErrSessionNil)
	}
	if len(m.commands) != 0 {
		t.Fatal("nil session was enqueued")
	}
}
