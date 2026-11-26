package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestEmitWritesOneJSONLine(t *testing.T) {
	var buf bytes.Buffer
	Emit(&buf, "ready", map[string]any{"url": "http://10.0.0.5:49152/index.m3u8"})
	line := buf.String()
	if !strings.HasSuffix(line, "\n") || strings.Count(line, "\n") != 1 {
		t.Fatalf("want exactly one newline-terminated line, got %q", line)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if got["event"] != "ready" || got["url"] != "http://10.0.0.5:49152/index.m3u8" {
		t.Fatalf("wrong payload: %v", got)
	}
}

func TestEmitNilFields(t *testing.T) {
	var buf bytes.Buffer
	Emit(&buf, "stopped", nil)
	if strings.TrimSpace(buf.String()) != `{"event":"stopped"}` {
		t.Fatalf("got %q", buf.String())
	}
}

// A sealed instance is how a superseded cast goes quiet: after Seal, progress
// and "packaged" events belonging to its cast must never reach the next cast's
// log, while the closing "stopped" (and a terminal "error") still must.
func TestProtocolSealSilencesEverythingButTerminalEvents(t *testing.T) {
	var buf bytes.Buffer
	p := NewProtocol(&buf, "")
	p.Seal()
	p.Emit("progress", map[string]any{"pct": 42})
	p.Emit("packaged", nil)
	p.Emit("ready", map[string]any{"url": "http://x/index.m3u8"})
	if buf.Len() != 0 {
		t.Fatalf("a sealed instance emitted non-terminal events:\n%s", buf.String())
	}
	p.Emit("stopped", nil)
	p.Emit("error", map[string]any{"msg": "boom"})
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want stopped and error, got:\n%s", buf.String())
	}
}

// Every event carries the cast's token so a consumer can tell casts apart.
func TestProtocolStampsToken(t *testing.T) {
	var buf bytes.Buffer
	p := NewProtocol(&buf, "cast-a")
	fields := map[string]any{"pct": 1}
	p.Emit("progress", fields)
	if _, leaked := fields["token"]; leaked {
		t.Fatal("Emit mutated the caller's map")
	}
	var ev map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &ev); err != nil {
		t.Fatal(err)
	}
	if ev["token"] != "cast-a" || ev["event"] != "progress" {
		t.Fatalf("event not stamped: %v", ev)
	}
}
