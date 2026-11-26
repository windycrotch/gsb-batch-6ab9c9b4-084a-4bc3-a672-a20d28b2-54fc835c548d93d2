package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// bundledHelperEnv names the environment variable that points the suite at an
// already-built helper — the universal binary inside a packed .iinaplgz —
// instead of compiling one from source. packaging/test-package.sh sets it.
const bundledHelperEnv = "IINA_AIRPLAY_HELPER"

// locateHelper is the pure decision behind buildHelper: given the value of
// IINA_AIRPLAY_HELPER and a stat function, it returns either the binary to use,
// a request to build one, or a fatal message. It takes no *testing.T so the
// decision can be table-tested with fake paths, mirroring locateSystemFFmpeg in
// e2e_test.go.
//
// A set-but-unusable path is FATAL, never a fall back to `go build`: the reason
// to set this variable at all is to exercise the shipped binary, so a silent
// fallback would let a typo be mistaken for a passing bundled-binary run.
func locateHelper(env string, stat func(string) error) (path string, build bool, fatal string) {
	if env == "" {
		return "", true, ""
	}
	if err := stat(env); err != nil {
		return "", false, fmt.Sprintf("%s=%q is not usable: %v", bundledHelperEnv, env, err)
	}
	return env, false, ""
}

// Builds the helper binary once for integration tests — or returns the bundled
// one when IINA_AIRPLAY_HELPER points at it. Every test that calls this then
// drives the shipped universal binary, not just the e2e ones, so the watchdog,
// the pidfile takeover and the stdout protocol get exercised on whichever
// architecture is running the suite.
func buildHelper(t *testing.T) string {
	t.Helper()
	path, build, fatal := locateHelper(os.Getenv(bundledHelperEnv), func(p string) error {
		_, err := os.Stat(p)
		return err
	})
	if fatal != "" {
		t.Fatal(fatal)
	}
	if !build {
		return path
	}
	bin := filepath.Join(t.TempDir(), "airplay-helper")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func TestLocateHelper(t *testing.T) {
	statOK := func(string) error { return nil }
	statMissing := func(string) error { return errors.New("no such file or directory") }

	t.Run("unset builds from source", func(t *testing.T) {
		path, build, fatal := locateHelper("", statMissing)
		if !build || path != "" || fatal != "" {
			t.Fatalf("got (%q, %v, %q), want (\"\", true, \"\")", path, build, fatal)
		}
	})

	t.Run("set and present is used as-is", func(t *testing.T) {
		path, build, fatal := locateHelper("/pkg/bin/airplay-helper", statOK)
		if path != "/pkg/bin/airplay-helper" || build || fatal != "" {
			t.Fatalf("got (%q, %v, %q), want the bundled path with no build and no fatal", path, build, fatal)
		}
	})

	// The whole point of setting this variable is to exercise the SHIPPED
	// binary. A silent fallback to `go build` would let a typo in the CI
	// workflow read as a passing bundled-binary run — which is precisely the
	// failure the Intel job exists to rule out. Same contract as findFFmpeg.
	t.Run("set but unusable is fatal, never a fallback", func(t *testing.T) {
		_, build, fatal := locateHelper("/nope/airplay-helper", statMissing)
		if build {
			t.Fatal("a bad IINA_AIRPLAY_HELPER fell back to building from source; a typo must fail loudly")
		}
		if fatal == "" {
			t.Fatal("want a fatal message, got none")
		}
		for _, want := range []string{"IINA_AIRPLAY_HELPER", "/nope/airplay-helper"} {
			if !strings.Contains(fatal, want) {
				t.Errorf("fatal message %q does not mention %q", fatal, want)
			}
		}
	})
}

func TestServeEndToEndWithStub(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	// Stub ffmpeg: writes a playlist like the real one would, then idles like a
	// long transcode so `serve` stays up.
	stub := filepath.Join(t.TempDir(), "ffmpeg")
	os.WriteFile(stub, []byte(fmt.Sprintf(
		"#!/bin/sh\necho '#EXTM3U' > %s/index.m3u8\necho out_time_us=1000000\ntrap 'exit 0' TERM\nsleep 60\n",
		out)), 0o755)

	cmd := exec.Command(bin, "serve",
		"-source", "/dev/null", "-out", out, "-ffmpeg", stub,
		"-parent", fmt.Sprint(os.Getpid()), "-duration", "60",
		"-vcodec", "h264", "-acodec", "aac", "-achannels", "2", "-vmap", "0", "-amap", "1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()

	var readyURL string
	sc := bufio.NewScanner(stdout)
	deadline := time.After(15 * time.Second)
	for readyURL == "" {
		lineCh := make(chan string, 1)
		go func() {
			if sc.Scan() {
				lineCh <- sc.Text()
			} else {
				lineCh <- ""
			}
		}()
		select {
		case line := <-lineCh:
			if line == "" {
				t.Fatal("helper stdout closed before ready")
			}
			var ev map[string]any
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("bad line %q", line)
			}
			if ev["event"] == "ready" {
				readyURL, _ = ev["url"].(string)
			}
			if ev["event"] == "error" {
				t.Fatalf("helper errored: %v", ev)
			}
		case <-deadline:
			t.Fatal("no ready event within 15s")
		}
	}
	if !strings.HasSuffix(readyURL, "/index.m3u8") || strings.Contains(readyURL, "127.0.0.1") {
		t.Fatalf("bad ready url %q", readyURL)
	}
	resp, err := http.Get(readyURL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("playlist fetch: %d", resp.StatusCode)
	}
	// pidfile exists while serving
	if _, err := os.Stat(filepath.Join(out, "helper.pid")); err != nil {
		t.Fatal("pidfile missing while serving")
	}

	// `stop` kills the serving instance
	if err := exec.Command(bin, "stop", "-out", out).Run(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serve instance survived stop")
	}
}

func TestServeWithSubsAdvertisesRewrittenMaster(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	// Stub ffmpeg writes what hlsenc would for one variant + a subtitle
	// rendition, then idles like a long transcode. All three files
	// (index.m3u8, index_vtt.m3u8, master.m3u8) are required before the
	// helper's ready gate fires — leaving any one out here would make
	// waitReady below time out (verified by hand: dropping the
	// index_vtt.m3u8 line makes this test fail with "no ready event in
	// time" instead of passing).
	stub := filepath.Join(t.TempDir(), "ffmpeg")
	master := `#EXTM3U\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="default",NAME="subtitle_0",DEFAULT=NO,AUTOSELECT=YES,URI="index_vtt.m3u8"\nindex.m3u8`
	os.WriteFile(stub, []byte(fmt.Sprintf(
		"#!/bin/sh\necho '#EXTM3U' > %[1]s/index.m3u8\necho '#EXTM3U' > %[1]s/index_vtt.m3u8\nprintf '%%b\\n' '%[2]s' > %[1]s/master.m3u8\necho out_time_us=1000000\ntrap 'exit 0' TERM\nsleep 60\n",
		out, master)), 0o755)

	cmd := exec.Command(bin, "serve",
		"-source", "/dev/null", "-out", out, "-ffmpeg", stub,
		"-parent", fmt.Sprint(os.Getpid()), "-duration", "60",
		"-vcodec", "h264", "-acodec", "aac", "-achannels", "2", "-vmap", "0", "-amap", "1",
		"-smap", "2", "-sublang", "en", "-subname", "English")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()

	readyURL := waitReady(t, stdout, 15*time.Second)
	if !strings.HasSuffix(readyURL, "/master.m3u8") {
		t.Fatalf("with subs the helper must advertise the master playlist, got %q", readyURL)
	}
	resp, err := http.Get(readyURL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	s := string(body)
	if !strings.Contains(s, "DEFAULT=YES") || !strings.Contains(s, `NAME="English"`) || !strings.Contains(s, `LANGUAGE="en"`) {
		t.Fatalf("served master not rewritten:\n%s", s)
	}
}

func TestServeCleansStaleSubtitleFiles(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	// Stale leftovers from a previous subtitled cast; this cast has no subs,
	// so nothing recreates them — they must be gone once serving is up.
	for _, f := range []string{"master.m3u8", "index_vtt.m3u8", "seg_0000.vtt"} {
		os.WriteFile(filepath.Join(out, f), []byte("stale"), 0o644)
	}
	stub := filepath.Join(t.TempDir(), "ffmpeg")
	os.WriteFile(stub, []byte(fmt.Sprintf(
		"#!/bin/sh\necho '#EXTM3U' > %s/index.m3u8\ntrap 'exit 0' TERM\nsleep 60\n",
		out)), 0o755)

	cmd := exec.Command(bin, "serve",
		"-source", "/dev/null", "-out", out, "-ffmpeg", stub,
		"-parent", fmt.Sprint(os.Getpid()), "-duration", "60",
		"-vcodec", "h264", "-acodec", "aac", "-achannels", "2", "-vmap", "0", "-amap", "1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()

	waitReady(t, stdout, 15*time.Second)
	for _, f := range []string{"master.m3u8", "index_vtt.m3u8", "seg_0000.vtt"} {
		if _, err := os.Stat(filepath.Join(out, f)); err == nil {
			t.Fatalf("stale %s survived cleanup", f)
		}
	}
}

// waitReady scans helper stdout until a ready event and returns its url.
func waitReady(t *testing.T, stdout io.Reader, timeout time.Duration) string {
	t.Helper()
	sc := bufio.NewScanner(stdout)
	deadline := time.After(timeout)
	for {
		lineCh := make(chan string, 1)
		go func() {
			if sc.Scan() {
				lineCh <- sc.Text()
			} else {
				lineCh <- ""
			}
		}()
		select {
		case line := <-lineCh:
			if line == "" {
				t.Fatal("helper stdout closed before ready")
			}
			var ev map[string]any
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("bad line %q", line)
			}
			if ev["event"] == "error" {
				t.Fatalf("helper errored: %v", ev)
			}
			if ev["event"] == "ready" {
				url, _ := ev["url"].(string)
				return url
			}
		case <-deadline:
			t.Fatal("no ready event in time")
		}
	}
}

// castFiles is what a real cast leaves in the output directory: the
// playlist, the fMP4 init segment and at least one media segment. Every
// teardown path must remove all of them (issue #12: a stopped cast used to
// leave the whole remux in temp until the next cast).
var castFiles = []string{"index.m3u8", "init.mp4", "seg_0000.m4s"}

// castingStub writes a stub ffmpeg that lays down castFiles in out and then
// runs tail — how the stub idles or exits.
func castingStub(t *testing.T, out, tail string) string {
	t.Helper()
	stub := filepath.Join(t.TempDir(), "ffmpeg")
	script := fmt.Sprintf("#!/bin/sh\necho '#EXTM3U' > %[1]s/index.m3u8\necho init > %[1]s/init.mp4\necho seg > %[1]s/seg_0000.m4s\n%[2]s\n", out, tail)
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return stub
}

func startServe(t *testing.T, bin, out, stub string, parent int) (*exec.Cmd, io.Reader) {
	t.Helper()
	return startServeWith(t, bin, out, stub, parent, "")
}

func startServeWith(t *testing.T, bin, out, stub string, parent int, token string) (*exec.Cmd, io.Reader) {
	t.Helper()
	args := []string{"serve",
		"-source", "/dev/null", "-out", out, "-ffmpeg", stub,
		"-parent", fmt.Sprint(parent), "-duration", "60",
		"-vcodec", "h264", "-acodec", "aac", "-achannels", "2", "-vmap", "0", "-amap", "1"}
	if token != "" {
		args = append(args, "-token", token)
	}
	cmd := exec.Command(bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	return cmd, stdout
}

func waitExit(t *testing.T, cmd *exec.Cmd, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("helper still running after %v", timeout)
	}
}

func assertSwept(t *testing.T, out string) {
	t.Helper()
	for _, f := range append(append([]string{}, castFiles...), "helper.pid") {
		if _, err := os.Stat(filepath.Join(out, f)); err == nil {
			t.Errorf("%s left behind in the output dir", f)
		}
	}
}

func TestStopSweepsOutDir(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	stub := castingStub(t, out, "trap 'exit 0' TERM\nsleep 60")
	cmd, stdout := startServe(t, bin, out, stub, os.Getpid())
	waitReady(t, stdout, 15*time.Second)
	for _, f := range castFiles {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Fatalf("stub did not write %s: %v", f, err)
		}
	}

	if err := exec.Command(bin, "stop", "-out", out).Run(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	waitExit(t, cmd, 5*time.Second)
	assertSwept(t, out)
}

// After ffmpeg has finished (VOD complete) the helper keeps serving until
// killed — a distinct branch of the serve loop that must sweep too.
func TestStopAfterPackagedSweepsOutDir(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	stub := castingStub(t, out, "echo progress=end\nexit 0")
	cmd, stdout := startServe(t, bin, out, stub, os.Getpid())
	waitReady(t, stdout, 15*time.Second)
	// Give the serve loop time to consume ffmpeg's exit before stopping.
	time.Sleep(500 * time.Millisecond)

	if err := exec.Command(bin, "stop", "-out", out).Run(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	waitExit(t, cmd, 5*time.Second)
	assertSwept(t, out)
}

// IINA quitting (or crashing) is the other way a cast ends; the watchdog
// path must sweep just like the signal path.
func TestParentDeathSweepsOutDir(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	parent := exec.Command("/bin/sleep", "60")
	if err := parent.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { parent.Process.Kill(); parent.Wait() })
	stub := castingStub(t, out, "trap 'exit 0' TERM\nsleep 60")
	cmd, stdout := startServe(t, bin, out, stub, parent.Process.Pid)
	waitReady(t, stdout, 15*time.Second)

	parent.Process.Kill()
	parent.Wait()
	waitExit(t, cmd, 10*time.Second) // watchdog polls every 2s
	assertSwept(t, out)
}

// A cast that fails part-way leaves partial segments; the error exit must
// sweep them as well.
func TestFailedCastSweepsOutDir(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	stub := castingStub(t, out, "echo 'boom' >&2\nexit 1")
	cmd, stdout := startServe(t, bin, out, stub, os.Getpid())
	b, _ := io.ReadAll(stdout)
	waitExit(t, cmd, 10*time.Second)
	sawError := false
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var ev map[string]any
		if json.Unmarshal([]byte(line), &ev) == nil && ev["event"] == "error" {
			sawError = true
		}
	}
	if !sawError {
		t.Fatalf("expected an error event, got:\n%s", b)
	}
	assertSwept(t, out)
}

// ---- handover: a new cast takes over from a live one ----

// eventsOf splits captured helper stdout into decoded events.
func eventsOf(t *testing.T, b []byte) []map[string]any {
	t.Helper()
	var evs []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var ev map[string]any
		if json.Unmarshal([]byte(line), &ev) == nil && ev["event"] != nil {
			evs = append(evs, ev)
		}
	}
	return evs
}

// The handover the user sees: cast A is live and serving, cast B starts on the
// same output directory. B must not announce ready until A is gone, and A's
// files must be swept and replaced by B's own — so B's URL does not briefly
// serve A's playlist and then 404 when A sweeps it.
func TestTakeoverReplacesLiveCastCleanly(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	stubA := castingStub(t, out, "trap 'exit 0' TERM\nsleep 60")
	first, stdoutA := startServeWith(t, bin, out, stubA, os.Getpid(), "cast-a")
	waitReady(t, stdoutA, 15*time.Second)

	// B's stub tags its playlist so the two casts are distinguishable.
	stubB := filepath.Join(t.TempDir(), "ffmpeg")
	os.WriteFile(stubB, []byte(fmt.Sprintf(
		"#!/bin/sh\necho '#EXTM3U' > %s/index.m3u8\necho '#EXTM3U-B' > /dev/null\necho init > %s/init.mp4\necho seg > %s/seg_0000.m4s\ntrap 'exit 0' TERM\nsleep 60\n",
		out, out, out)), 0o755)
	second, stdoutB := startServeWith(t, bin, out, stubB, os.Getpid(), "cast-b")
	urlB := waitReady(t, stdoutB, 20*time.Second)

	if !strings.HasSuffix(urlB, "/index.m3u8") {
		t.Fatalf("bad ready url %q", urlB)
	}
	// B announced ready only after A exited; A must be gone by now.
	waitExit(t, first, 5*time.Second)
	resp, err := http.Get(urlB)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("the new cast's playlist 404s right after ready: %d", resp.StatusCode)
	}
	// A's event stream ends at "stopped" and carries no events after it.
	rest, _ := io.ReadAll(stdoutA)
	for _, ev := range eventsOf(t, rest) {
		if ev["event"] != "stopped" {
			t.Errorf("superseded cast emitted %v after handing over", ev)
		}
	}

	// B is the live cast; its stop sweeps and leaves nothing behind.
	if err := exec.Command(bin, "stop", "-out", out, "-token", "cast-b").Run(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	waitExit(t, second, 5*time.Second)
	assertSwept(t, out)
}

// A superseded instance must go quiet the moment it is told to stop: no
// progress or "packaged" events belonging to its cast may land in the next
// cast's log (the mixed-log symptom).
func TestSupersededCastEmitsNothingAfterStopSignal(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	// A stub that keeps emitting progress right up to the SIGTERM, so there is
	// something to leak if sealing fails.
	stub := castingStub(t, out, "trap 'exit 0' TERM\nwhile true; do echo out_time_us=1000000; sleep 0.05; done")
	cmd, stdout := startServeWith(t, bin, out, stub, os.Getpid(), "cast-a")
	waitReady(t, stdout, 15*time.Second)

	cmd.Process.Signal(syscall.SIGTERM)
	// Read to EOF before Wait: cmd.Wait closes the pipe, so anything buffered
	// in it would otherwise be lost.
	rest, _ := io.ReadAll(stdout)
	waitExit(t, cmd, 5*time.Second)
	evs := eventsOf(t, rest)
	sawStopped := false
	sawAfter := false
	for _, ev := range evs {
		if ev["event"] == "stopped" {
			sawStopped = true
			continue
		}
		if sawStopped {
			sawAfter = true
		}
	}
	if !sawStopped {
		t.Fatalf("no stopped event:\n%s", rest)
	}
	if sawAfter {
		t.Fatalf("events after stopped (the cast narrated into the next one):\n%s", rest)
	}
}

// A stop aimed at a cast that is no longer serving is a no-op: it must not
// kill the newer instance nor sweep its files.
func TestStaleStopLeavesNewerCastAlone(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	stub := castingStub(t, out, "trap 'exit 0' TERM\nsleep 60")
	cmd, stdout := startServeWith(t, bin, out, stub, os.Getpid(), "cast-new")
	waitReady(t, stdout, 15*time.Second)

	if err := exec.Command(bin, "stop", "-out", out, "-token", "cast-old").Run(); err != nil {
		t.Fatalf("a stale stop must exit 0, got %v", err)
	}
	// The new cast is untouched: still running, still serving its files. Its
	// process handle is probed with kill(0), not Wait — the later stop needs
	// Wait's single caller.
	if !processAlive(cmd.Process.Pid) {
		t.Fatal("a stale stop killed the newer cast")
	}
	for _, f := range castFiles {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Fatalf("a stale stop swept the newer cast's %s", f)
		}
	}
	// And its own stop still works.
	if err := exec.Command(bin, "stop", "-out", out, "-token", "cast-new").Run(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	waitExit(t, cmd, 5*time.Second)
	assertSwept(t, out)
}

// TestTakeoverFailsWhenPredecessorNeverExits: a predecessor that keeps the
// directory locked past the grace must fail the new instance outright — with
// an error event, a non-zero exit, and the predecessor's files left alone.
func TestTakeoverFailsWhenPredecessorNeverExits(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	// A real second process holds the directory and ignores SIGTERM: the wedged
	// predecessor. Its pidfile is written by startLockHolder.
	h := startLockHolder(t, out, "cast-old", true)
	os.WriteFile(filepath.Join(out, "index.m3u8"), []byte("#EXTM3U\n"), 0o644)

	cmd := exec.Command(bin, "serve",
		"-source", "/dev/null", "-out", out, "-ffmpeg", "/bin/true",
		"-parent", fmt.Sprint(os.Getpid()), "-duration", "60",
		"-vcodec", "h264", "-acodec", "aac", "-achannels", "2", "-vmap", "0", "-amap", "1",
		"-token", "cast-new")
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	b, _ := io.ReadAll(stdout)
	if err := cmd.Wait(); err == nil {
		t.Fatal("the new instance must exit non-zero when it cannot take over")
	}
	if !strings.Contains(string(b), `"event":"error"`) {
		t.Fatalf("expected an error event, got:\n%s", b)
	}
	// The wedged predecessor's files and pidfile are untouched.
	if _, err := os.Stat(filepath.Join(out, "index.m3u8")); err != nil {
		t.Fatal("a failed takeover swept the predecessor's files")
	}
	if _, err := os.Stat(filepath.Join(out, "helper.pid")); err != nil {
		t.Fatal("a failed takeover removed the predecessor's pidfile")
	}
	if !processAlive(h.pid()) {
		t.Fatal("the takeover killed a predecessor that was holding the directory")
	}
}

// The ready URL must never serve a superseded cast's playlist. Cast B announces
// ready only after it has taken the directory and swept A's files, so a TV that
// fetches the URL the instant it is advertised gets B's playlist — not A's
// (which A would then delete underneath the fetch: the "404 right after the
// address arrived" symptom).
func TestReadyURLNeverServesThePreviousCastsPlaylist(t *testing.T) {
	bin := buildHelper(t)
	out := t.TempDir()
	// A writes a recognisable playlist and keeps running until taken over.
	stubA := filepath.Join(t.TempDir(), "ffmpeg")
	os.WriteFile(stubA, []byte(fmt.Sprintf(
		"#!/bin/sh\necho '#EXTM3U' > %[1]s/index.m3u8\necho '#OLD-CAST' >> %[1]s/index.m3u8\necho init > %[1]s/init.mp4\necho seg > %[1]s/seg_0000.m4s\ntrap 'exit 0' TERM\nsleep 60\n",
		out)), 0o755)
	first, stdoutA := startServeWith(t, bin, out, stubA, os.Getpid(), "cast-a")
	waitReady(t, stdoutA, 15*time.Second)

	// B blocks its first playlist until the test says so, which pins the window
	// where a handover could wrongly advertise A's still-present file.
	gate := filepath.Join(t.TempDir(), "gate")
	stubB := filepath.Join(t.TempDir(), "ffmpeg")
	os.WriteFile(stubB, []byte(fmt.Sprintf(
		"#!/bin/sh\nwhile [ ! -f %[3]s ]; do sleep 0.05; done\necho '#EXTM3U' > %[1]s/index.m3u8\necho init > %[1]s/init.mp4\necho seg > %[1]s/seg_0000.m4s\ntrap 'exit 0' TERM\nsleep 60\n",
		out, out, gate)), 0o755)
	second, stdoutB := startServeWith(t, bin, out, stubB, os.Getpid(), "cast-b")

	// B has been started but has not written anything; A has been asked to stop.
	// Nothing may be advertised yet — and A's playlist must already be gone by
	// the time B's own appears, so there is no instant at which the eventual
	// URL serves A's file.
	waitExit(t, first, 10*time.Second)
	os.WriteFile(gate, []byte("go"), 0o644)
	urlB := waitReady(t, stdoutB, 20*time.Second)
	body := getBody(t, urlB)
	if strings.Contains(body, "#OLD-CAST") {
		t.Fatalf("the new cast announced a playlist that was still the old cast's:\n%s", body)
	}
	if !strings.Contains(body, "#EXTM3U") {
		t.Fatalf("implausible playlist at ready:\n%s", body)
	}
	second.Process.Kill()
	second.Wait()
}
