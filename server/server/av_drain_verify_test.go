package server_test

// Acceptance verification for drain / migration ownership and cleanup
// consistency. Everything runs against real httptest HTTP + gorilla
// WebSocket entry points; Discord is replaced by an injectable voice conn and
// audio source factory. All identifiers carry the av prefix so this file can
// coexist with independently written grader tests.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	botgateway "github.com/disgoorg/disgo/gateway"
	discordvoice "github.com/disgoorg/disgo/voice"
	"github.com/disgoorg/godave"
	"github.com/disgoorg/snowflake/v2"
	"github.com/gorilla/websocket"
	"github.com/shi-gg/linkdave/server/audio/filter"
	"github.com/shi-gg/linkdave/server/audio/source"
	"github.com/shi-gg/linkdave/server/protocol"
	srv "github.com/shi-gg/linkdave/server/server"
	"github.com/shi-gg/linkdave/server/voice"
)

const (
	avGuildID   = snowflake.ID(123456789012345678)
	avChannelID = snowflake.ID(234567890123456789)
	avClientID  = snowflake.ID(345678901234567890)
	avTrackURL  = "http://example.invalid/track.mp3"
	avFrameMs   = 20
)

type avFakeVoiceConn struct {
	mu          sync.Mutex
	guildID     snowflake.ID
	stateUpdate discordvoice.StateUpdateFunc
	removeConn  func()
	provider    discordvoice.OpusFrameProvider
	closed      bool
}

func (f *avFakeVoiceConn) Gateway() discordvoice.Gateway                             { return nil }
func (f *avFakeVoiceConn) UDP() discordvoice.UDPConn                                 { return nil }
func (f *avFakeVoiceConn) DAVE() godave.Session                                      { return nil }
func (f *avFakeVoiceConn) ChannelID() *snowflake.ID                                  { return nil }
func (f *avFakeVoiceConn) GuildID() snowflake.ID                                     { return f.guildID }
func (f *avFakeVoiceConn) UserIDBySSRC(uint32) snowflake.ID                          { return 0 }
func (f *avFakeVoiceConn) SetOpusFrameReceiver(discordvoice.OpusFrameReceiver)       {}
func (f *avFakeVoiceConn) HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate)   {}
func (f *avFakeVoiceConn) HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate) {}

func (f *avFakeVoiceConn) SetSpeaking(context.Context, discordvoice.SpeakingFlags) error {
	return nil
}

func (f *avFakeVoiceConn) SetOpusFrameProvider(provider discordvoice.OpusFrameProvider) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.provider = provider
}

func (f *avFakeVoiceConn) Open(context.Context, snowflake.ID, bool, bool) error {
	return nil
}

func (f *avFakeVoiceConn) Close(context.Context) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	stateUpdate := f.stateUpdate
	removeConn := f.removeConn
	guildID := f.guildID
	f.mu.Unlock()

	_ = stateUpdate(context.Background(), guildID, nil, false, false)
	removeConn()
}

func (f *avFakeVoiceConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *avFakeVoiceConn) pullFrame() ([]byte, error) {
	f.mu.Lock()
	provider := f.provider
	f.mu.Unlock()
	if provider == nil {
		return nil, fmt.Errorf("no opus provider")
	}
	return provider.ProvideOpusFrame()
}

type avFakeSource struct {
	url      string
	position int64
	closed   bool
	mu       sync.Mutex
}

func (s *avFakeSource) ProvideOpusFrame() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil
	}
	s.position += avFrameMs
	return []byte{0xF8, 0xFF, 0xFE}, nil
}
func (s *avFakeSource) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}
func (s *avFakeSource) Position() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.position
}
func (s *avFakeSource) SeekTo(ms int64) error {
	s.mu.Lock()
	s.position = ms
	s.mu.Unlock()
	return nil
}
func (s *avFakeSource) Duration() int64 { return 60000 }
func (s *avFakeSource) CanSeek() bool   { return true }
func (s *avFakeSource) URL() string     { return s.url }
func (s *avFakeSource) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

type avSourceFactory struct {
	mu      sync.Mutex
	sources []*avFakeSource
}

func (f *avSourceFactory) CreateFromURL(_ context.Context, url string, start int64, _ *filter.Filters) (source.Source, error) {
	src := &avFakeSource{url: url, position: start}
	f.mu.Lock()
	f.sources = append(f.sources, src)
	f.mu.Unlock()
	return src, nil
}

func (f *avSourceFactory) snapshot() []*avFakeSource {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*avFakeSource, len(f.sources))
	copy(out, f.sources)
	return out
}

func (f *avSourceFactory) last() *avFakeSource {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sources) == 0 {
		return nil
	}
	return f.sources[len(f.sources)-1]
}

type avEnv struct {
	t       *testing.T
	server  *srv.Server
	manager *voice.Manager
	http    *httptest.Server
	factory *avSourceFactory

	mu    sync.Mutex
	conns []*avFakeVoiceConn
}

func avNewEnv(t *testing.T) *avEnv {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := voice.NewManager(logger)
	factory := &avSourceFactory{}

	env := &avEnv{t: t, manager: manager, factory: factory}
	manager.SetConnCreateFunc(env.createConn)
	manager.SetSourceFactory(factory)

	env.server = srv.NewServer(logger, manager, "av-test", "")

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", env.server.HandleWebSocket)
	env.server.RegisterRoutes(mux)
	env.http = httptest.NewServer(mux)

	t.Cleanup(func() {
		env.http.Close()
		manager.Close()
	})

	return env
}

func (e *avEnv) createConn(guildID, _ snowflake.ID, stateUpdate discordvoice.StateUpdateFunc, removeConn func(), _ ...discordvoice.ConnConfigOpt) discordvoice.Conn {
	conn := &avFakeVoiceConn{guildID: guildID, stateUpdate: stateUpdate, removeConn: removeConn}
	e.mu.Lock()
	e.conns = append(e.conns, conn)
	e.mu.Unlock()
	return conn
}

func (e *avEnv) lastConn() *avFakeVoiceConn {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.conns) == 0 {
		return nil
	}
	return e.conns[len(e.conns)-1]
}

func (e *avEnv) post(path string, body any) *http.Response {
	e.t.Helper()
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			e.t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(payload)
	}
	resp, err := http.Post(e.http.URL+path, "application/json", reader)
	if err != nil {
		e.t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func (e *avEnv) postRaw(path string, raw string) *http.Response {
	e.t.Helper()
	resp, err := http.Post(e.http.URL+path, "application/json", strings.NewReader(raw))
	if err != nil {
		e.t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func (e *avEnv) delete(path string) *http.Response {
	e.t.Helper()
	req, err := http.NewRequest(http.MethodDelete, e.http.URL+path, nil)
	if err != nil {
		e.t.Fatalf("delete req: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("DELETE %s: %v", path, err)
	}
	return resp
}

type avMsg struct {
	Op   uint8           `json:"op"`
	Data json.RawMessage `json:"d"`
}

type avWS struct {
	t         *testing.T
	conn      *websocket.Conn
	writeMu   sync.Mutex
	incoming  chan avMsg
	stashed   []avMsg
	SessionID string
}

func (e *avEnv) dial() *avWS {
	e.t.Helper()
	wsURL := "ws" + strings.TrimPrefix(e.http.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		e.t.Fatalf("dial: %v", err)
	}
	c := &avWS{t: e.t, conn: conn, incoming: make(chan avMsg, 512)}
	e.t.Cleanup(func() { conn.Close() })

	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				close(c.incoming)
				return
			}
			var msg avMsg
			if json.Unmarshal(data, &msg) == nil {
				c.incoming <- msg
			}
		}
	}()

	ready := c.waitFor(protocol.OpReady, 2*time.Second)
	var rd protocol.ReadyData
	if err := json.Unmarshal(ready.Data, &rd); err != nil {
		e.t.Fatalf("ready: %v", err)
	}
	c.SessionID = rd.SessionID
	return c
}

func (c *avWS) send(op uint8, data any) {
	c.t.Helper()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	payload, _ := json.Marshal(data)
	raw, _ := json.Marshal(struct {
		Op   uint8           `json:"op"`
		Data json.RawMessage `json:"d"`
	}{op, payload})
	if err := c.conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		c.t.Fatalf("send op %d: %v", op, err)
	}
}

func (c *avWS) waitFor(op uint8, timeout time.Duration) avMsg {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for i, m := range c.stashed {
			if m.Op == op {
				c.stashed = append(c.stashed[:i], c.stashed[i+1:]...)
				return m
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.t.Fatalf("timeout waiting op %d", op)
		}
		select {
		case m, ok := <-c.incoming:
			if !ok {
				c.t.Fatalf("conn closed waiting op %d", op)
			}
			if m.Op == op {
				return m
			}
			c.stashed = append(c.stashed, m)
		case <-time.After(remaining):
			c.t.Fatalf("timeout waiting op %d", op)
		}
	}
}

func (c *avWS) drain(window time.Duration) []avMsg {
	out := c.stashed
	c.stashed = nil
	timer := time.NewTimer(window)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-c.incoming:
			if !ok {
				return out
			}
			out = append(out, m)
		case <-timer.C:
			return out
		}
	}
}

func avWait(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (e *avEnv) connectVoice(c *avWS, g snowflake.ID) {
	c.send(protocol.OpVoiceUpdate, protocol.VoiceUpdateData{
		ClientID: avClientID, GuildID: g, ChannelID: avChannelID,
		SessionID: "discord-session",
		Event:     protocol.VoiceServerEvent{Token: "t", GuildID: g.String(), Endpoint: "ep"},
	})
	c.waitFor(protocol.OpVoiceConnect, 2*time.Second)
}

func (e *avEnv) play(c *avWS, g snowflake.ID, body protocol.RequestPlay) {
	resp := e.post(fmt.Sprintf("/sessions/%s/players/%d/play", c.SessionID, g), body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		e.t.Fatalf("play status %d: %s", resp.StatusCode, string(b))
	}
	c.waitFor(protocol.OpTrackStart, 2*time.Second)
}

func (e *avEnv) migrate(c *avWS, g snowflake.ID) protocol.MigrateReadyData {
	c.send(protocol.OpPlayerMigrate, protocol.PlayerMigrateData{GuildID: g})
	msg := c.waitFor(protocol.OpMigrateReady, 2*time.Second)
	var d protocol.MigrateReadyData
	if err := json.Unmarshal(msg.Data, &d); err != nil {
		e.t.Fatalf("migrate data: %v", err)
	}
	return d
}

func avPullFrames(t *testing.T, conn *avFakeVoiceConn, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		frame, err := conn.pullFrame()
		if err != nil {
			t.Fatalf("pull: %v", err)
		}
		if frame == nil {
			t.Fatalf("expected frame, got silence")
		}
	}
}

func avReadError(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	var body protocol.ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return body.Error
}

// A1: once draining, new plays are explicitly rejected with 503 + a draining
// error, and the player count does not grow.
func TestAVDrainRejectsNewPlay(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	env.connectVoice(c, avGuildID)
	env.play(c, avGuildID, protocol.RequestPlay{URL: avTrackURL})

	if got := env.server.PlayerCount(); got != 1 {
		t.Fatalf("precondition: count=%d", got)
	}

	env.server.Drain("shutdown", 5000)
	c.waitFor(protocol.OpNodeDraining, 2*time.Second)

	resp := env.post(fmt.Sprintf("/sessions/%s/players/%d/play", c.SessionID, avGuildID),
		protocol.RequestPlay{URL: "http://example.invalid/other.mp3"})
	if resp.StatusCode < 400 {
		t.Fatalf("expected rejection, got %d", resp.StatusCode)
	}
	if msg := strings.ToLower(avReadError(t, resp)); !strings.Contains(msg, "drain") {
		t.Fatalf("expected draining error, got %q", msg)
	}

	for _, op := range []struct {
		path string
		body any
	}{
		{"pause", nil},
		{"resume", nil},
		{"stop", nil},
	} {
		r := env.post(fmt.Sprintf("/sessions/%s/players/%d/%s", c.SessionID, avGuildID, op.path), op.body)
		if r.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s during drain: status %d", op.path, r.StatusCode)
		}
		r.Body.Close()
	}

	if got := env.server.PlayerCount(); got != 1 {
		t.Fatalf("count changed after rejected ops: %d", got)
	}
}

// A8: ordinary REST lifecycle keeps the baseline contract when not draining.
func TestAVBaselineRestLifecycle(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	env.connectVoice(c, avGuildID)
	env.play(c, avGuildID, protocol.RequestPlay{URL: avTrackURL, RequesterID: "req-1"})

	r := env.post(fmt.Sprintf("/sessions/%s/players/%d/pause", c.SessionID, avGuildID), nil)
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("pause %d", r.StatusCode)
	}
	r.Body.Close()
	u := c.waitFor(protocol.OpPlayerUpdate, 2*time.Second)
	var upd protocol.PlayerUpdateData
	json.Unmarshal(u.Data, &upd)
	if upd.State != protocol.PlayerStatePaused {
		t.Fatalf("state=%s", upd.State)
	}

	r = env.post(fmt.Sprintf("/sessions/%s/players/%d/resume", c.SessionID, avGuildID), nil)
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("resume %d", r.StatusCode)
	}
	r.Body.Close()
	u = c.waitFor(protocol.OpPlayerUpdate, 2*time.Second)
	json.Unmarshal(u.Data, &upd)
	if upd.State != protocol.PlayerStatePlaying {
		t.Fatalf("state=%s", upd.State)
	}

	r = env.post(fmt.Sprintf("/sessions/%s/players/%d/seek", c.SessionID, avGuildID), protocol.RequestSeek{Position: 1000})
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("seek %d", r.StatusCode)
	}
	r.Body.Close()

	r = env.post(fmt.Sprintf("/sessions/%s/players/%d/stop", c.SessionID, avGuildID), nil)
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("stop %d", r.StatusCode)
	}
	r.Body.Close()
	end := c.waitFor(protocol.OpTrackEnd, 2*time.Second)
	var te protocol.TrackEndData
	json.Unmarshal(end.Data, &te)
	if te.Reason != protocol.TrackEndReasonStopped {
		t.Fatalf("reason=%s", te.Reason)
	}
	c.waitFor(protocol.OpPlayerUpdate, 2*time.Second)

	r = env.delete(fmt.Sprintf("/sessions/%s/players/%d", c.SessionID, avGuildID))
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("delete %d", r.StatusCode)
	}
	r.Body.Close()
}

// A3: the handshake snapshot matches real source position, state, requester
// and filters at the moment of the handshake.
func TestAVMigrateSnapshotMatchesPlayback(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	env.connectVoice(c, avGuildID)

	playBody := `{"url":"` + avTrackURL + `","requester_id":"user-42","filters":{"enabled":[1]}}`
	resp := env.postRaw(fmt.Sprintf("/sessions/%s/players/%d/play", c.SessionID, avGuildID), playBody)
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("play %d %s", resp.StatusCode, b)
	}
	resp.Body.Close()
	c.waitFor(protocol.OpTrackStart, 2*time.Second)

	avPullFrames(t, env.lastConn(), 5)
	if got := env.factory.last().Position(); got != 100 {
		t.Fatalf("src pos %d", got)
	}

	snap := env.migrate(c, avGuildID)
	if snap.Position != 100 || snap.State != protocol.PlayerStatePlaying {
		t.Fatalf("snapshot pos=%d state=%s", snap.Position, snap.State)
	}
	if snap.RequesterID != "user-42" {
		t.Fatalf("requester=%q", snap.RequesterID)
	}
	if snap.Filters == nil || len(snap.Filters.Enabled) != 1 || snap.Filters.Enabled[0] != filter.Nightcore {
		t.Fatalf("filters=%+v", snap.Filters)
	}
	if got := env.manager.Position(c.SessionID, avGuildID); got != 100 {
		t.Fatalf("manager pos %d", got)
	}

	// Abort, pause, then migrate again: state/position must follow pause.
	c.send(protocol.OpPlayerMigrateAbort, protocol.PlayerMigrateData{GuildID: avGuildID})
	c.waitFor(protocol.OpPlayerUpdate, 2*time.Second)

	r := env.post(fmt.Sprintf("/sessions/%s/players/%d/pause", c.SessionID, avGuildID), nil)
	r.Body.Close()
	c.waitFor(protocol.OpPlayerUpdate, 2*time.Second)

	snap = env.migrate(c, avGuildID)
	if snap.State != protocol.PlayerStatePaused || snap.Position != 100 {
		t.Fatalf("paused snapshot pos=%d state=%s", snap.Position, snap.State)
	}
	c.send(protocol.OpPlayerMigrateAbort, protocol.PlayerMigrateData{GuildID: avGuildID})
	c.waitFor(protocol.OpPlayerUpdate, 2*time.Second)

	r = env.post(fmt.Sprintf("/sessions/%s/players/%d/seek", c.SessionID, avGuildID), protocol.RequestSeek{Position: 250})
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("seek %d", r.StatusCode)
	}
	r.Body.Close()

	snap = env.migrate(c, avGuildID)
	if snap.Position != 250 {
		t.Fatalf("seek snapshot pos=%d", snap.Position)
	}
}

// A4: concurrent/repeated handshakes all return the same single state.
func TestAVMigrateRepeatedIsIdempotent(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	env.connectVoice(c, avGuildID)
	env.play(c, avGuildID, protocol.RequestPlay{URL: avTrackURL})
	avPullFrames(t, env.lastConn(), 5)

	const n = 6
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.send(protocol.OpPlayerMigrate, protocol.PlayerMigrateData{GuildID: avGuildID})
		}()
	}
	wg.Wait()

	var first *protocol.MigrateReadyData
	for i := 0; i < n; i++ {
		msg := c.waitFor(protocol.OpMigrateReady, 2*time.Second)
		var d protocol.MigrateReadyData
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = &d
			continue
		}
		if d != *first {
			t.Fatalf("divergent snapshot %+v vs %+v", d, *first)
		}
	}

	if got := env.server.PlayerCount(); got != 1 {
		t.Fatalf("player should stay registered during handoff, count=%d", got)
	}
}

// A5: after commit, late disconnect/track-end callbacks are silenced.
func TestAVMigrateCommitSilencesCleanup(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	env.connectVoice(c, avGuildID)
	env.play(c, avGuildID, protocol.RequestPlay{URL: avTrackURL})
	avPullFrames(t, env.lastConn(), 3)
	src := env.factory.last()

	env.migrate(c, avGuildID)

	// A direct disconnect callback while the handoff is pending is swallowed.
	env.server.OnVoiceDisconnected(c.SessionID, avGuildID)

	c.send(protocol.OpPlayerMigrateCommit, protocol.PlayerMigrateData{GuildID: avGuildID})
	avWait(t, "count zero", func() bool { return env.server.PlayerCount() == 0 })
	avWait(t, "conn closed", func() bool { return env.lastConn().isClosed() })

	// Late callbacks from old-node cleanup must not reach the client.
	env.server.OnVoiceDisconnected(c.SessionID, avGuildID)
	env.server.OnTrackEnd(c.SessionID, avGuildID, src, protocol.TrackEndReasonStopped)

	for _, m := range c.drain(400 * time.Millisecond) {
		if m.Op == protocol.OpVoiceDisconnect || m.Op == protocol.OpTrackEnd || m.Op == protocol.OpTrackError {
			t.Fatalf("unexpected event op %d after handoff", m.Op)
		}
	}
}

// A5/A6: losing the voice connection mid-handoff keeps the player silent and
// aborting resumes playback from the frozen position.
func TestAVVoiceLossDuringHandoffThenAbort(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	env.connectVoice(c, avGuildID)
	env.play(c, avGuildID, protocol.RequestPlay{URL: avTrackURL})
	conn := env.lastConn()
	avPullFrames(t, conn, 5)

	snap := env.migrate(c, avGuildID)
	if snap.Position != 100 {
		t.Fatalf("pos %d", snap.Position)
	}

	conn.Close(context.Background())

	// Outlive the 1s unexpected-disconnect timer.
	for _, m := range c.drain(2200 * time.Millisecond) {
		if m.Op == protocol.OpVoiceDisconnect || m.Op == protocol.OpTrackEnd || m.Op == protocol.OpTrackError {
			t.Fatalf("unexpected op %d while handoff in flight", m.Op)
		}
	}
	if got := env.server.PlayerCount(); got != 1 {
		t.Fatalf("player lost during handoff, count=%d", got)
	}

	c.send(protocol.OpPlayerMigrateAbort, protocol.PlayerMigrateData{GuildID: avGuildID})
	c.waitFor(protocol.OpPlayerUpdate, 2*time.Second)

	avWait(t, "manager conn back", func() bool {
		return env.manager.HasConnection(c.SessionID, avGuildID)
	})
	avPullFrames(t, env.lastConn(), 1)
	if got := env.factory.last().Position(); got != 120 {
		t.Fatalf("position after abort %d", got)
	}
	if got := env.manager.Position(c.SessionID, avGuildID); got != 120 {
		t.Fatalf("manager position %d", got)
	}
}

// A6: cancelling drain rejects no more, resumes in-flight handoffs and new
// plays succeed; positions never regress.
func TestAVUndrainRestoresService(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	env.connectVoice(c, avGuildID)
	env.play(c, avGuildID, protocol.RequestPlay{URL: avTrackURL})
	avPullFrames(t, env.lastConn(), 5)

	env.server.Drain("shutdown", 5000)
	c.waitFor(protocol.OpNodeDraining, 2*time.Second)

	r := env.post(fmt.Sprintf("/sessions/%s/players/%d/play", c.SessionID, avGuildID),
		protocol.RequestPlay{URL: "http://example.invalid/x.mp3"})
	if r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("drain play status %d", r.StatusCode)
	}
	r.Body.Close()

	snap := env.migrate(c, avGuildID)
	if snap.Position != 100 {
		t.Fatalf("snap %d", snap.Position)
	}

	env.server.Undrain()
	c.waitFor(protocol.OpPlayerUpdate, 2*time.Second)

	avPullFrames(t, env.lastConn(), 1)
	if got := env.manager.Position(c.SessionID, avGuildID); got < 100 {
		t.Fatalf("position regressed to %d", got)
	}

	// Service accepts new plays again.
	env.play(c, avGuildID, protocol.RequestPlay{URL: "http://example.invalid/new.mp3"})
}

// A2: drain racing plays yields only fully-mounted or fully-rejected plays;
// no source is created for a rejected admission.
func TestAVDrainPlayRace(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	env.connectVoice(c, avGuildID)
	env.play(c, avGuildID, protocol.RequestPlay{URL: avTrackURL})

	const writers = 12
	var wg sync.WaitGroup
	statuses := make(chan int, writers+1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		env.server.Drain("race", 5000)
	}()

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := env.post(fmt.Sprintf("/sessions/%s/players/%d/play", c.SessionID, avGuildID),
				protocol.RequestPlay{URL: fmt.Sprintf("http://example.invalid/%d.mp3", i)})
			_, _ = io.Copy(io.Discard, r.Body)
			r.Body.Close()
			statuses <- r.StatusCode
		}(i)
	}
	wg.Wait()
	close(statuses)

	var okCount, rejected int
	for st := range statuses {
		switch st {
		case http.StatusNoContent:
			okCount++
		case http.StatusServiceUnavailable:
			rejected++
		default:
			t.Fatalf("unexpected status %d", st)
		}
	}
	if okCount+rejected != writers {
		t.Fatalf("counts %d/%d", okCount, rejected)
	}

	sources := env.factory.snapshot()
	// Every created source corresponds to a 204 admission (rejections happen
	// before source creation).
	if len(sources) != okCount+1 { // +1 for the initial play
		t.Fatalf("created sources=%d but mounted plays=%d", len(sources), okCount+1)
	}
	// Replaced sources must be closed; only the newest may stay open.
	for i, src := range sources {
		if i < len(sources)-1 && !src.isClosed() {
			t.Fatalf("source %d leaked open after replacement", i)
		}
	}
	if got := env.server.PlayerCount(); got != 1 {
		t.Fatalf("count %d", got)
	}
}

// A7: concurrent voice updates, plays and disconnects for one session/guild
// never leave residual manager connections; run under -race.
func TestAVConcurrentUpdatesPlayDisconnect(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()

	var wg sync.WaitGroup

	// Voice updates stay on the ordered WebSocket and are acknowledged before
	// the goroutine reports done, so no update can land after the final
	// disconnect below.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			c.send(protocol.OpVoiceUpdate, protocol.VoiceUpdateData{
				ClientID: avClientID, GuildID: avGuildID, ChannelID: avChannelID,
				SessionID: "ds", Event: protocol.VoiceServerEvent{Token: "t", Endpoint: "e"},
			})
			c.waitFor(protocol.OpVoiceConnect, 2*time.Second)
		}
	}()

	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			r := env.post(fmt.Sprintf("/sessions/%s/players/%d/play", c.SessionID, avGuildID),
				protocol.RequestPlay{URL: fmt.Sprintf("http://example.invalid/%d.mp3", i)})
			_, _ = io.Copy(io.Discard, r.Body)
			r.Body.Close()
			if r.StatusCode != http.StatusNoContent && r.StatusCode != http.StatusNotFound {
				t.Errorf("play status %d", r.StatusCode)
			}
		}(i)
		go func() {
			defer wg.Done()
			r := env.delete(fmt.Sprintf("/sessions/%s/players/%d", c.SessionID, avGuildID))
			_, _ = io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}()
	}
	wg.Wait()

	// The acknowledged voice stream ends connected; one final disconnect must
	// remove the only map entry for the session/guild pair.
	r := env.delete(fmt.Sprintf("/sessions/%s/players/%d", c.SessionID, avGuildID))
	r.Body.Close()

	avWait(t, "manager connections drained", func() bool {
		return env.manager.ConnectionCount() == 0
	})

	// Exactly one connection per session/guild was ever current: the manager
	// map is keyed by session:guild, so assert the invariant directly.
	if env.manager.HasConnection(c.SessionID, avGuildID) {
		t.Fatal("connection still registered after final disconnect")
	}
}

func TestAVMigrateUnknownPlayerFails(t *testing.T) {
	env := avNewEnv(t)
	c := env.dial()
	c.send(protocol.OpPlayerMigrate, protocol.PlayerMigrateData{GuildID: snowflake.ID(999)})
	msg := c.waitFor(protocol.OpMigrateFailed, 2*time.Second)
	var failed protocol.MigrateFailedData
	if err := json.Unmarshal(msg.Data, &failed); err != nil {
		t.Fatal(err)
	}
	if failed.Reason == "" || failed.GuildID != 999 {
		t.Fatalf("bad failed payload %+v", failed)
	}
}
