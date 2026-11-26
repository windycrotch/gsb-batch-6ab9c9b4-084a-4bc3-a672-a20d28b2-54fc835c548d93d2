package adler_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/catalinfl/adler"
	"github.com/lxzan/gws"
)

func TestCloseWithCodeSendsCodeAndReason(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   uint16
		reason string
		want   string
	}{
		{name: "normal closure", code: 1000},
		{name: "custom code", code: 4001, reason: "session expired", want: "session expired"},
		{name: "at byte limit", code: 4001, reason: strings.Repeat("a", 123), want: strings.Repeat("a", 123)},
		{name: "long ASCII", code: 4001, reason: strings.Repeat("a", 200), want: strings.Repeat("a", 123)},
		{name: "split two-byte rune", code: 4001, reason: strings.Repeat("a", 122) + "ț", want: strings.Repeat("a", 122)},
		{name: "split four-byte rune", code: 4001, reason: strings.Repeat("a", 121) + "😀", want: strings.Repeat("a", 121)},
		{name: "complete final rune", code: 4001, reason: strings.Repeat("a", 121) + "țmore", want: strings.Repeat("a", 121) + "ț"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := adler.New()
			disconnected := make(chan struct{}, 1)
			a.HandleDisconnect(func(*adler.Session) { disconnected <- struct{}{} })
			c, s := setupSingleSession(t, a)

			if err := s.CloseWithCode(tc.code, tc.reason); err != nil {
				t.Fatalf("close with code: %v", err)
			}
			waitSignal(t, c.handler.errc, "client did not receive the close frame")
			var closeErr *gws.CloseError
			if !errors.As(c.handler.err, &closeErr) {
				t.Fatalf("expected a close frame, got: %v", c.handler.err)
			}
			if closeErr.Code != tc.code || string(closeErr.Reason) != tc.want || !utf8.Valid(closeErr.Reason) {
				t.Fatalf("unexpected close frame: code=%d reason=%q", closeErr.Code, closeErr.Reason)
			}
			waitSignal(t, disconnected, "close handshake did not disconnect the session")
			if err := s.CloseWithCode(tc.code, tc.reason); !errors.Is(err, adler.ErrSessionClosed) {
				t.Fatalf("close disconnected session: got=%v want=%v", err, adler.ErrSessionClosed)
			}
		})
	}
}

func TestCloseWithCodeDisconnectsUnresponsivePeer(t *testing.T) {
	a := adler.New()
	sessions := make(chan *adler.Session, 1)
	disconnected := make(chan struct{}, 1)
	a.HandleConnect(func(s *adler.Session) { sessions <- s })
	a.HandleDisconnect(func(*adler.Session) { disconnected <- struct{}{} })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = a.HandleRequest(w, r)
	}))
	defer srv.Close()

	// Without a read loop the peer cannot answer the close handshake.
	conn, _, err := gws.NewClient(gws.BuiltinEventHandler{}, &gws.ClientOption{Addr: wsURL(srv.URL)})
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.NetConn().Close()
	s := waitSession(t, sessions, "session not captured")
	if err := s.CloseWithCode(4001, "session expired"); err != nil {
		t.Fatalf("close with code: %v", err)
	}

	select {
	case <-disconnected:
	case <-time.After(4 * time.Second):
		t.Fatal("unresponsive peer was not disconnected after the close timeout")
	}
	if err := s.WriteText([]byte("after close")); !errors.Is(err, adler.ErrSessionClosed) {
		t.Fatalf("write after forced close: got=%v want=%v", err, adler.ErrSessionClosed)
	}
}
