package main

import (
	"encoding/json"
	"io"
	"sync"
)

var emitMu sync.Mutex

// Emit writes one JSON event line. The JS side splits helper stdout on
// newlines, so an event must never span or share a line.
func Emit(w io.Writer, event string, fields map[string]any) {
	emitMu.Lock()
	defer emitMu.Unlock()
	emitLine(w, event, fields)
}

// emitLine is Emit's body. It is split out so Protocol can serialize its own
// writes without taking emitMu twice.
func emitLine(w io.Writer, event string, fields map[string]any) {
	obj := map[string]any{"event": event}
	for k, v := range fields {
		obj[k] = v
	}
	b, err := json.Marshal(obj)
	if err != nil {
		b = []byte(`{"event":"error","msg":"marshal failure"}`)
	}
	w.Write(append(b, '\n'))
}

// Protocol is the helper's stdout event stream. Unlike a bare io.Writer it can
// be sealed: once this instance has been told to stop, it must not keep
// narrating the cast it is tearing down. A superseded instance's progress and
// "packaged" events used to arrive after the next cast had started, mixing one
// cast's log into the other's. Sealing drops everything except the closing
// "stopped" (or a terminal "error"), which is how the plugin learns how the
// teardown ended.
type Protocol struct {
	mu     sync.Mutex
	w      io.Writer
	token  string
	sealed bool
}

// terminalEvent is true for the events a sealed instance may still send: the
// end of its cast, never the middle of it.
func terminalEvent(event string) bool { return event == "stopped" || event == "error" }

// NewProtocol returns the event stream for one cast. token, when non-empty, is
// stamped on every event so the consumer can tell this cast's events from a
// previous one's that a rearranged pipe delivered late.
func NewProtocol(w io.Writer, token string) *Protocol {
	return &Protocol{w: w, token: token}
}

// Write lets a *Protocol stand in for the io.Writer RunJob takes (so its
// progress goroutine can be switched off by the same seal).
func (p *Protocol) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.w.Write(b)
}

func (p *Protocol) Emit(event string, fields map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sealed && !terminalEvent(event) {
		return
	}
	if p.token != "" {
		// Copy rather than mutate the caller's map: RunJob builds a fresh one
		// per event, but callers outside it need not.
		withToken := make(map[string]any, len(fields)+1)
		for k, v := range fields {
			withToken[k] = v
		}
		withToken["token"] = p.token
		fields = withToken
	}
	emitLine(p.w, event, fields)
}

// Seal stops all further events but "stopped". Idempotent, and safe to call
// from the signal goroutine while the serve loop is emitting.
func (p *Protocol) Seal() {
	p.mu.Lock()
	p.sealed = true
	p.mu.Unlock()
}

// eventSink is the emit surface RunJob needs. *Protocol implements it; a plain
// io.Writer (as the job tests pass) does not, so sinkFor wraps it.
type eventSink interface {
	Emit(event string, fields map[string]any)
}

func sinkFor(w io.Writer) func(string, map[string]any) {
	if s, ok := w.(eventSink); ok {
		return s.Emit
	}
	return func(event string, fields map[string]any) { Emit(w, event, fields) }
}
