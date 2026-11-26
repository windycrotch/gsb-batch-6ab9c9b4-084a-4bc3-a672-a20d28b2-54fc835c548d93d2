package adler_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/catalinfl/adler"
	"github.com/lxzan/gws"
)

func TestDisconnectHooksRunInOrderBeforeHandleDisconnect(t *testing.T) {
	a := adler.New(adler.WithDispatchAsync(false))

	var order []string
	events := make(chan string, 4)

	a.AddDisconnectHook(func(*adler.Session) { events <- "first" })
	a.AddDisconnectHook(func(*adler.Session) { events <- "second" })

	sessions := make(chan *adler.Session, 1)
	a.HandleConnect(func(s *adler.Session) { sessions <- s })
	// Registered after the hooks on purpose: the callback slot is separate.
	a.HandleDisconnect(func(*adler.Session) { events <- "handler" })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = a.HandleRequest(w, r)
	}))
	defer srv.Close()

	conn, _, err := gws.NewClient(gws.BuiltinEventHandler{}, &gws.ClientOption{Addr: wsURL(srv.URL)})
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	go conn.ReadLoop()
	_ = waitSession(t, sessions, "session not captured")

	if err := conn.NetConn().Close(); err != nil {
		t.Fatalf("disconnect client: %v", err)
	}

	timeout := time.After(2 * time.Second)
	for range 3 {
		select {
		case name := <-events:
			order = append(order, name)
		case <-timeout:
			t.Fatalf("disconnect callbacks did not all run: got=%v", order)
		}
	}

	want := []string{"first", "second", "handler"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("callback order: got=%v want=%v", order, want)
		}
	}
}

func TestHandleDisconnectReplacementKeepsHooks(t *testing.T) {
	a := adler.New(adler.WithDispatchAsync(false))

	var hookCalls, replacedCalls, finalCalls atomic.Int32
	a.AddDisconnectHook(func(*adler.Session) { hookCalls.Add(1) })
	a.HandleDisconnect(func(*adler.Session) { replacedCalls.Add(1) })
	// Replacing the disconnect handler must not remove the additive hook.
	a.HandleDisconnect(func(*adler.Session) { finalCalls.Add(1) })

	c, _ := setupSingleSession(t, a)
	if err := c.Close(); err != nil {
		t.Fatalf("disconnect client: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for hookCalls.Load() == 0 || finalCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("disconnect cleanup not complete: hook=%d handler=%d", hookCalls.Load(), finalCalls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := hookCalls.Load(); got != 1 {
		t.Fatalf("disconnect hook calls: got=%d want=1", got)
	}
	if got := replacedCalls.Load(); got != 0 {
		t.Fatalf("replaced disconnect handler ran: got=%d want=0", got)
	}
	if got := finalCalls.Load(); got != 1 {
		t.Fatalf("final disconnect handler calls: got=%d want=1", got)
	}
}

func TestAddDisconnectHookIgnoresNil(t *testing.T) {
	a := adler.New()
	a.AddDisconnectHook(nil)
}
