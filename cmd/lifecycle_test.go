package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ffmpeg-api/api"
)

var fakeFFmpegPath string

// defaultPayloadSize matches the fake FFmpeg default output size.
const defaultPayloadSize = 256

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "fakeffmpeg-build-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "test setup failed:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	name := "ffmpeg"
	if isWindows() {
		name = "ffmpeg.exe"
	}
	built := filepath.Join(dir, name)
	cmd := exec.Command("go", "build", "-o", built, "./testdata/fakeffmpeg")
	cmd.Dir = mustGetwd()
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building fake ffmpeg failed: %v\n%s\n", err, output)
		os.Exit(1)
	}
	fakeFFmpegPath = built
	os.Exit(m.Run())
}

func isWindows() bool {
	return os.PathSeparator == '\\'
}

func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return wd
}

// harness wires a request entry point to an isolated job directory root and
// the controllable fake FFmpeg binary.
type harness struct {
	t         *testing.T
	server    *httptest.Server
	source    *httptest.Server
	jobRoot   string
	ffmpeLog  string
	heartbeat string

	// slowStarted counts /slow requests that began streaming;
	// slowCanceled counts those whose client/server context ended early.
	slowStarted  atomic.Int64
	slowCanceled atomic.Int64

	// postCancelWrites counts response header/body writes attempted after the
	// request context was already canceled by the server.
	postCancelWrites atomic.Int64
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	jobRoot := filepath.Join(dir, "jobs")
	logPath := filepath.Join(dir, "ffmpeg.log")

	t.Setenv("FFMPEG_BINARY", fakeFFmpegPath)
	t.Setenv("FAKE_FFMPEG_LOG", logPath)
	t.Setenv("FAKE_FFMPEG_DIR", "")
	t.Setenv("FAKE_FFMPEG_HEARTBEAT", filepath.Join(dir, "ffmpeg-heartbeat"))

	old := jobTempRoot
	jobTempRoot = func() string { return jobRoot }
	t.Cleanup(func() { jobTempRoot = old })

	h := &harness{t: t, jobRoot: jobRoot, ffmpeLog: logPath, heartbeat: filepath.Join(dir, "ffmpeg-heartbeat")}
	h.server = httptest.NewServer(h.wrap(newMux()))
	t.Cleanup(h.server.Close)
	h.source = httptest.NewServer(h.sourceHandler())
	t.Cleanup(h.source.Close)
	return h
}

// wrap instruments the real handler's ResponseWriter so the test can observe
// any attempt to emit a status or body after the request was canceled.
func (h *harness) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&observeWriter{ResponseWriter: w, ctx: r.Context(), h: h}, r)
	})
}

type observeWriter struct {
	http.ResponseWriter
	ctx    context.Context
	h      *harness
	wrote  atomic.Bool
	status bool
}

func (w *observeWriter) WriteHeader(status int) {
	if w.ctx.Err() != nil && !w.status {
		w.h.postCancelWrites.Add(1)
	}
	w.status = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *observeWriter) Write(p []byte) (int, error) {
	if w.ctx.Err() != nil && len(p) > 0 {
		w.h.postCancelWrites.Add(1)
	}
	w.wrote.Store(true)
	return w.ResponseWriter.Write(p)
}

func (w *observeWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func (h *harness) sourceHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/good", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, strings.NewReader("input-payload"))
	})
	mux.HandleFunc("/fail", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "unavailable")
	})
	mux.HandleFunc("/hang", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(30 * time.Second):
		}
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		flusher, _ := w.(http.Flusher)
		h.slowStarted.Add(1)
		for i := 0; ; i++ {
			select {
			case <-r.Context().Done():
				h.slowCanceled.Add(1)
				return
			default:
			}
			fmt.Fprintf(w, "chunk-%d\n", i)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
	return mux
}

func (h *harness) process(t *testing.T, req api.ProcessRequest) (*http.Response, []byte) {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(h.server.URL+"/v1/process", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body failed: %v", err)
	}
	return resp, data
}

// rawProcess sends a request over a bare connection and returns the
// connection so a test can disconnect while the handler is still running.
func (h *harness) rawProcess(t *testing.T, req api.ProcessRequest) net.Conn {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", strings.TrimPrefix(h.server.URL, "http://"))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	request := fmt.Sprintf("POST /v1/process HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatalf("writing request failed: %v", err)
	}
	return conn
}

// jobDirs lists working directories that currently exist under the harness
// root. A finished request must leave none behind.
func (h *harness) jobDirs(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(h.jobRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("reading job root failed: %v", err)
	}
	var dirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(h.jobRoot, entry.Name()))
		}
	}
	return dirs
}

func (h *harness) requireNoJobDirs(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if dirs := h.jobDirs(t); len(dirs) == 0 {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("job directories were not removed: %v", dirs)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// logSize returns the current size of the fake FFmpeg activity log.
func (h *harness) logSize(t *testing.T) int64 {
	t.Helper()
	info, err := os.Stat(h.ffmpeLog)
	if err != nil {
		return 0
	}
	return info.Size()
}

// heartbeatSize returns the current size of the fake FFmpeg heartbeat file.
func (h *harness) heartbeatSize(t *testing.T) int64 {
	t.Helper()
	info, err := os.Stat(h.heartbeat)
	if err != nil {
		return 0
	}
	return info.Size()
}

// requireBeating proves the fake FFmpeg process is alive and actively
// emitting heartbeats, so a later liveness check is meaningful.
func (h *harness) requireBeating(t *testing.T) {
	t.Helper()
	waitFor(t, 3*time.Second, "fake ffmpeg heartbeat", func() bool {
		return h.heartbeatSize(t) > 0
	})
	first := h.heartbeatSize(t)
	waitFor(t, 3*time.Second, "fake ffmpeg progress", func() bool {
		return h.heartbeatSize(t) > first
	})
}

// requireBeatStops proves the fake FFmpeg process is no longer writing
// heartbeats, i.e. it was actually terminated rather than left running.
func (h *harness) requireBeatStops(t *testing.T) {
	t.Helper()
	first := h.heartbeatSize(t)
	time.Sleep(400 * time.Millisecond)
	second := h.heartbeatSize(t)
	if second != first {
		t.Fatalf("fake ffmpeg kept running after cancellation: heartbeat grew %d -> %d", first, second)
	}
}

func streamRequest(source string, extra ...string) api.ProcessRequest {
	args := append([]string{"-i", "input.txt"}, extra...)
	args = append(args, "-f", "mp3", "pipe:1")
	return api.ProcessRequest{
		Inputs:   map[string]api.Input{"input.txt": {HTTP: source}},
		Commands: [][]string{args},
		Output:   api.Output{Stream: true, InlineContentType: "audio/mpeg"},
	}
}

func base64Request(source string, commands ...[]string) api.ProcessRequest {
	if len(commands) == 0 {
		commands = [][]string{{"-i", "input.txt", "-codec", "copy", "output.mp3"}}
	}
	return api.ProcessRequest{
		Inputs:   map[string]api.Input{"input.txt": {HTTP: source}},
		Commands: commands,
		Output:   api.Output{Base64: true},
	}
}

func TestStreamSuccessReportsOkAndActualBytes(t *testing.T) {
	h := newHarness(t)
	req := streamRequest(h.source.URL + "/good")
	resp, body := h.process(t, req)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Trailer.Get(statusTrailer); got != "ok" {
		t.Fatalf("%s = %q, want ok", statusTrailer, got)
	}
	if got := resp.Trailer.Get(bytesTrailer); got != fmt.Sprint(len(body)) {
		t.Fatalf("%s = %q, want %d (actual body length)", bytesTrailer, got, len(body))
	}
	if len(body) != defaultPayloadSize {
		t.Fatalf("body length = %d, want %d", len(body), defaultPayloadSize)
	}
	h.requireNoJobDirs(t)
}

func TestStreamFailureReportsErrorAndPartialBytes(t *testing.T) {
	h := newHarness(t)
	req := streamRequest(h.source.URL+"/good", "fake-fail")
	resp, body := h.process(t, req)

	// The body has already started, so the failure is expressed via trailers
	// rather than a different HTTP status.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Trailer.Get(statusTrailer); got != "error" {
		t.Fatalf("%s = %q, want error", statusTrailer, got)
	}
	if got := resp.Trailer.Get(bytesTrailer); got != fmt.Sprint(len(body)) {
		t.Fatalf("%s = %q, want %d counted from bytes actually written", bytesTrailer, got, len(body))
	}
	if len(body) != defaultPayloadSize/2 {
		t.Fatalf("partial body length = %d, want %d", len(body), defaultPayloadSize/2)
	}
	h.requireNoJobDirs(t)
}

func TestNonStreamFailureReturnsErrorAndRemovesDirectory(t *testing.T) {
	h := newHarness(t)
	req := base64Request(h.source.URL+"/good",
		[]string{"-i", "input.txt", "-codec", "copy", "intermediate.wav"},
		[]string{"-i", "intermediate.wav", "fake-fail", "output.mp3"},
	)
	resp, body := h.process(t, req)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("error response was not JSON: %v (%q)", err, body)
	}
	if _, ok := payload["error"]; !ok {
		t.Fatalf("error response missing error field: %q", body)
	}
	if _, ok := payload["results"]; ok {
		t.Fatalf("failed request returned partial results: %q", body)
	}
	h.requireNoJobDirs(t)
}

func TestConcurrentDownloadFailureCancelsOthersAndRespondsOnce(t *testing.T) {
	h := newHarness(t)
	req := api.ProcessRequest{
		Inputs: map[string]api.Input{
			"a.txt": {HTTP: h.source.URL + "/slow"},
			"b.txt": {HTTP: h.source.URL + "/fail"},
		},
		Commands: [][]string{{"-i", "a.txt", "-codec", "copy", "out.mp3"}},
		Output:   api.Output{Base64: true},
	}
	resp, body := h.process(t, req)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("response was not a single JSON object: %v (%q)", err, body)
	}
	if _, ok := payload["results"]; ok {
		t.Fatalf("failed request returned results: %q", body)
	}
	// The sibling download must have been canceled by the failure rather than
	// left running to completion.
	waitFor(t, 5*time.Second, "sibling download cancellation", func() bool {
		return h.slowCanceled.Load() > 0
	})
	h.requireNoJobDirs(t)
}

// waitFor polls cond until it returns true or the deadline expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCancelDuringDownloadStopsTransferAndRemovesDirectory(t *testing.T) {
	h := newHarness(t)
	req := base64Request(h.source.URL + "/slow")
	conn := h.rawProcess(t, req)

	// The transfer must be in flight before we disconnect.
	waitFor(t, 2*time.Second, "slow download to start", func() bool {
		return h.slowStarted.Load() > 0
	})
	conn.Close()

	// Closing the client connection cancels the request context, which must
	// abort the in-flight download at the source. The job directory is still
	// cleaned up afterwards.
	waitFor(t, 5*time.Second, "source-side download cancellation", func() bool {
		return h.slowCanceled.Load() > 0
	})
	h.requireNoJobDirs(t)
}

func TestCancelDuringFFmpegStopsProcessAndRemovesDirectory(t *testing.T) {
	h := newHarness(t)
	req := base64Request(h.source.URL+"/good", []string{"-i", "input.txt", "fake-hang", "output.mp3"})
	conn := h.rawProcess(t, req)

	// The process must be running and heartbeating before we disconnect.
	h.requireBeating(t)
	conn.Close()

	// Cleanup finishing means the handler returned, i.e. cancellation and
	// termination had their chance to run before we check for liveness.
	h.requireNoJobDirs(t)
	h.requireBeatStops(t)
}

func TestCancelDuringStreamingStopsProcessAndRemovesDirectory(t *testing.T) {
	h := newHarness(t)
	req := streamRequest(h.source.URL+"/good", "fake-slow")
	conn := h.rawProcess(t, req)

	// Wait until the response has begun and media bytes are flowing.
	reader := bufio.NewReader(conn)
	waitFor(t, 3*time.Second, "streamed body to begin", func() bool {
		return h.logSize(t) > 0
	})
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("reading streamed bytes failed: %v", err)
	}
	conn.Close()

	h.requireNoJobDirs(t)
	h.requireBeatStops(t)
}

func TestCleanupIsIdempotentAndCancelRepeatsSafely(t *testing.T) {
	root := t.TempDir()
	old := jobTempRoot
	jobTempRoot = func() string { return root }
	defer func() { jobTempRoot = old }()

	path, cleanup, err := createJobPath()
	if err != nil {
		t.Fatalf("createJobPath failed: %v", err)
	}

	_, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cancel()
			cleanup()
		}()
	}
	wg.Wait()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("job directory %s still exists after repeated cleanup", path)
	}
	cancel()
	cleanup()
}

// TestCancelTerminatesOrphanedWorkerThatInheritsStdout covers tools that spawn
// worker processes sharing the stdout pipe. Terminating only the direct PID
// leaves the worker alive; it keeps the pipe open (hanging Wait) and keeps
// producing output after the caller has gone.
// TestNoResponseWritesAfterCancellation proves that a request failing after
// the client disconnected does not attempt to emit a status or error body.
func TestNoResponseWritesAfterCancellation(t *testing.T) {
	h := newHarness(t)
	// The download fails after the client is gone; the handler must not write.
	req := base64Request(h.source.URL + "/slow")
	conn := h.rawProcess(t, req)
	waitFor(t, 2*time.Second, "slow download to start", func() bool {
		return h.slowStarted.Load() > 0
	})
	conn.Close()

	h.requireNoJobDirs(t)
	if got := h.postCancelWrites.Load(); got != 0 {
		t.Fatalf("handler attempted %d response write(s) after cancellation", got)
	}
}

func TestCancelTerminatesOrphanedWorkerThatInheritsStdout(t *testing.T) {
	h := newHarness(t)
	req := streamRequest(h.source.URL+"/good", "fake-orphan-worker")
	conn := h.rawProcess(t, req)

	// Proof the parent and its worker are running before we disconnect.
	h.requireBeating(t)
	conn.Close()

	// Cleanup completing means the handler returned, i.e. cancellation and
	// process termination had their chance to run. Only then is it meaningful
	// to assert that the worker's heartbeat stopped.
	h.requireNoJobDirs(t)
	h.requireBeatStops(t)
}
