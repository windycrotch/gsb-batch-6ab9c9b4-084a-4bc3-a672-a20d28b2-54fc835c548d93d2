package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfg/ffmpegd/ffmpeg"
	"github.com/gorilla/websocket"
)

const testOrigin = "http://localhost:8080"

// wsClient is a connected browser-like client that records every status it
// receives.
type wsClient struct {
	conn *websocket.Conn

	mu   sync.Mutex
	msgs []Status
	wake chan struct{}
}

func dialClient(t *testing.T, url string) *wsClient {
	t.Helper()
	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	h := http.Header{"Origin": []string{testOrigin}}
	c, _, err := d.Dial(url, h)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	cl := &wsClient{conn: c, wake: make(chan struct{}, 1)}
	go func() {
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			var st Status
			if err := json.Unmarshal(data, &st); err != nil {
				return
			}
			cl.mu.Lock()
			cl.msgs = append(cl.msgs, st)
			cl.mu.Unlock()
			select {
			case cl.wake <- struct{}{}:
			default:
			}
		}
	}()
	return cl
}

func (c *wsClient) send(t *testing.T, msg Message) {
	t.Helper()
	if err := c.conn.WriteJSON(msg); err != nil {
		t.Fatalf("send: %v", err)
	}
}

func (c *wsClient) snapshot() []Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Status(nil), c.msgs...)
}

func (c *wsClient) waitFor(t *testing.T, what string, cond func([]Status) bool) []Status {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		got := c.snapshot()
		if cond(got) {
			return got
		}
		select {
		case <-c.wake:
		case <-deadline:
			t.Fatalf("timed out waiting for %s; got: %+v", what, got)
			return nil
		}
	}
}

// startWSServer serves the real websocket handler over a test server. Statuses
// are recorded in log and written to every connected client, exactly as the
// real server does.
func startWSServer(t *testing.T, encode encodeFunc, log *statusLog) string {
	t.Helper()
	// The upgrader checks the Origin against allowedOrigins, which Run sets from
	// the port. Tests use their own origin.
	old := allowedOrigins
	allowedOrigins = []string{testOrigin}
	t.Cleanup(func() { allowedOrigins = old })

	h := &hub{conns: make(map[*websocket.Conn]bool)}
	s := newSession(10, encode, func(st Status) {
		log.broadcast(st)
		h.broadcast(st)
	})
	t.Cleanup(s.shutdown)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveClient(w, r, h, s)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

// TestClientsSeeIdenticalStatusStream checks that every connected client sees
// the same ordered sequence for a run of jobs, and that no job reports both a
// cancellation and a completion.
func TestClientsSeeIdenticalStatusStream(t *testing.T) {
	started := make(chan struct{}, 3)
	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		started <- struct{}{}
		switch msg.Input {
		case "cancel-me":
			report(Status{Percent: 25})
			<-ctx.Done()
			report(Status{Percent: 60}) // must be dropped after cancellation
			return ffmpeg.ErrCancelled
		case "fail":
			return errFakeFail
		}
		report(Status{Percent: 50})
		return nil
	}
	log := newStatusLog()
	url := startWSServer(t, encode, log)
	a := dialClient(t, url)
	b := dialClient(t, url)

	// Sequence: one cancelled job, one failed job, one successful job.
	a.send(t, Message{Type: "encode", Input: "cancel-me", Output: "1"})
	<-started
	a.send(t, Message{Type: "cancel"})
	a.send(t, Message{Type: "encode", Input: "fail", Output: "2"})
	a.send(t, Message{Type: "encode", Input: "ok", Output: "3"})

	want := func(all []Status) bool { return len(finals(all)) == 3 }
	gotA := a.waitFor(t, "three finals", want)
	gotB := b.waitFor(t, "three finals", want)

	if len(gotA) != len(gotB) {
		t.Fatalf("clients saw different numbers of statuses: A=%d B=%d\nA=%+v\nB=%+v",
			len(gotA), len(gotB), gotA, gotB)
	}
	for i := range gotA {
		if gotA[i] != gotB[i] {
			t.Fatalf("status %d differs between clients: A=%+v B=%+v\nA=%+v\nB=%+v",
				i, gotA[i], gotB[i], gotA, gotB)
		}
	}

	// Exactly one final per job, and never a job that is both cancelled and
	// complete.
	perJob := map[uint64][]Status{}
	for _, st := range gotA {
		perJob[st.JobID] = append(perJob[st.JobID], st)
	}
	if len(perJob) != 3 {
		t.Fatalf("saw statuses for %d jobs, want 3: %+v", len(perJob), gotA)
	}
	for id, js := range perJob {
		var f []Status
		for _, st := range js {
			if isFinal(st) {
				f = append(f, st)
			}
		}
		if len(f) != 1 {
			t.Fatalf("job %d has %d final statuses, want 1: %+v", id, len(f), js)
		}
		if f[0].Cancelled && f[0].Percent == 100 {
			t.Fatalf("job %d reported both cancelled and complete: %+v", id, f[0])
		}
	}
}

var errFakeFail = &fakeError{"fake encode failure"}

type fakeError struct{ s string }

func (e *fakeError) Error() string { return e.s }

// TestClientsDoNotSeeOtherJobsProgressAcrossReconnect fires a job, lets it
// finish, then a new job, on a fresh connection, and checks that the new job's
// statuses start at its own id with no tail from the previous job.
func TestClientsDoNotSeeOtherJobsProgressAcrossReconnect(t *testing.T) {
	gate := make(chan struct{})
	var gateOnce sync.Once
	unblock := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(unblock) // registered before the session, runs before its shutdown

	encode := func(ctx context.Context, msg Message, report func(Status)) error {
		report(Status{Percent: 10})
		if msg.Input == "first" {
			return nil
		}
		<-gate
		report(Status{Percent: 20})
		return nil
	}
	log := newStatusLog()
	url := startWSServer(t, encode, log)
	a := dialClient(t, url)
	a.send(t, Message{Type: "encode", Input: "first"})
	a.waitFor(t, "first job done", func(all []Status) bool { return len(finals(all)) == 1 })

	b := dialClient(t, url)
	b.send(t, Message{Type: "encode", Input: "second"})
	b.waitFor(t, "second job progress", func(all []Status) bool { return len(all) > 0 })

	for _, st := range b.snapshot() {
		if st.JobID != 2 {
			t.Fatalf("new client saw a status for job %d, want only job 2: %+v", st.JobID, st)
		}
	}
	unblock()
	b.waitFor(t, "second job done", func(all []Status) bool { return len(finals(all)) == 1 })
}
