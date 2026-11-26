package adler_test

import (
	"errors"
	"testing"
	"time"

	"github.com/catalinfl/adler"
	"github.com/lxzan/gws"
)

func TestTimeoutOptionsPreserveDurations(t *testing.T) {
	a := adler.New(
		adler.WithWriteWait(10*time.Second),
		adler.WithPongWait(60*time.Second),
		adler.WithPingPeriod(54*time.Second),
	)

	if got := a.Config.WriteWait; got != 10*time.Second {
		t.Fatalf("write timeout: got=%v want=10s", got)
	}
	if got := a.Config.PongWait; got != 60*time.Second {
		t.Fatalf("pong timeout: got=%v want=60s", got)
	}
	if got := a.Config.PingPeriod; got != 54*time.Second {
		t.Fatalf("ping interval: got=%v want=54s", got)
	}
}

func TestReadMaxPayloadSizeDefaults(t *testing.T) {
	for _, size := range []int{-1, 0, 128 * 1024} {
		a := adler.New(adler.WithReadMaxPayloadSize(size))
		want := size
		if size <= 0 {
			want = 64 * 1024
		}
		if got := a.Config.ReadMaxPayloadSize; got != want {
			t.Fatalf("payload limit for %d: got=%d want=%d", size, got, want)
		}
	}
	if got := adler.New().Config.ReadMaxPayloadSize; got != 64*1024 {
		t.Fatalf("default payload limit: got=%d want=65536", got)
	}
}

func TestPongsRenewDeadlineAndInvokeHandler(t *testing.T) {
	const pongWait = 600 * time.Millisecond
	a := adler.New(
		adler.WithPongWait(pongWait),
		adler.WithPingPeriod(50*time.Millisecond),
		adler.WithDispatchAsync(false),
	)
	pongs := make(chan struct{}, 64)
	received := make(chan []byte, 1)
	a.HandlePong(func(*adler.Session) { pongs <- struct{}{} })
	a.HandleMessage(func(_ *adler.Session, payload []byte) {
		received <- append([]byte(nil), payload...)
	})
	c, _ := setupSingleSession(t, a)

	timer := time.NewTimer(2 * pongWait)
	defer timer.Stop()
	pongCount := 0
waiting:
	for {
		select {
		case <-pongs:
			pongCount++
		case <-c.handler.errc:
			t.Fatalf("connection closed despite pong replies: %v", c.handler.err)
		case <-timer.C:
			break waiting
		}
	}
	if pongCount < 2 {
		t.Fatalf("pong callback was not invoked repeatedly: got=%d", pongCount)
	}
	if err := c.write(gws.OpcodeText, []byte("still connected")); err != nil {
		t.Fatalf("write after initial deadline: %v", err)
	}
	if got := string(waitBytes(t, received, "message not received after initial deadline")); got != "still connected" {
		t.Fatalf("unexpected message: %q", got)
	}
}

func TestDisconnectRespectsEmptyRoomCleanup(t *testing.T) {
	for _, deleteOnEmpty := range []bool{true, false} {
		name := "keep room"
		if deleteOnEmpty {
			name = "delete room"
		}
		t.Run(name, func(t *testing.T) {
			a := adler.New(adler.WithDeleteRoomOnEmpty(deleteOnEmpty))
			room := a.NewRoom("lobby")
			disconnected := make(chan struct{}, 1)
			left := make(chan struct{}, 1)
			room.HandleLeave(func(*adler.Session) { left <- struct{}{} })
			a.HandleDisconnect(func(*adler.Session) { disconnected <- struct{}{} })
			c, s := setupSingleSession(t, a)
			if err := room.Join(s); err != nil {
				t.Fatalf("join room: %v", err)
			}
			if err := c.Close(); err != nil {
				t.Fatalf("disconnect client: %v", err)
			}
			waitSignal(t, disconnected, "disconnect handler not called")
			waitSignal(t, left, "room leave handler not called")
			if room.Len() != 0 || s.Room() != nil {
				t.Fatal("disconnected session still belongs to room")
			}
			got, err := a.Room("lobby")
			if deleteOnEmpty {
				if !errors.Is(err, adler.ErrRoomNotFound) {
					t.Fatalf("empty room was not deleted: %v", err)
				}
			} else if err != nil || got != room {
				t.Fatalf("empty room was not retained: room=%p err=%v", got, err)
			}
		})
	}
}

func TestInboundPayloadLimit(t *testing.T) {
	a := adler.New(
		adler.WithReadMaxPayloadSize(4),
		adler.WithDispatchAsync(false),
	)
	received := make(chan []byte, 2)
	disconnected := make(chan struct{}, 1)
	a.HandleMessage(func(_ *adler.Session, payload []byte) {
		received <- append([]byte(nil), payload...)
	})
	a.HandleDisconnect(func(*adler.Session) { disconnected <- struct{}{} })
	c, _ := setupSingleSession(t, a)

	if err := c.write(gws.OpcodeText, []byte("1234")); err != nil {
		t.Fatalf("write message at limit: %v", err)
	}
	if got := string(waitBytes(t, received, "message at limit was rejected")); got != "1234" {
		t.Fatalf("unexpected accepted message: %q", got)
	}
	if err := c.write(gws.OpcodeText, []byte("12345")); err != nil {
		t.Fatalf("write oversized message: %v", err)
	}
	waitSignal(t, disconnected, "oversized message did not close connection")
	select {
	case payload := <-received:
		t.Fatalf("oversized message reached handler: %q", payload)
	default:
	}
}
