package main

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The handover tests need a *separate* process holding the directory lock, the
// way a serving instance does: flock is per open file description, so an
// in-process second lock would not model contention at all, and a test
// goroutine releasing it races with the code under test. Re-executing the test
// binary as a lock holder gives a real second process whose death releases the
// lock exactly as a helper's does.

const (
	holderModeEnv  = "AIRPLAY_TEST_LOCK_HOLDER"
	holderDirEnv   = "AIRPLAY_TEST_LOCK_DIR"
	holderTokenEnv = "AIRPLAY_TEST_LOCK_TOKEN"
	holderReadyEnv = "AIRPLAY_TEST_LOCK_READY"
	holderStubEnv  = "AIRPLAY_TEST_LOCK_IGNORE_TERM"
)

// TestLockHolderProcess is not a test: when the holder env is set it takes the
// directory lock, records its pid, announces readiness and blocks until it is
// killed. With ignoreTerm it refuses SIGTERM, the "wedged predecessor" case.
func TestLockHolderProcess(t *testing.T) {
	if os.Getenv(holderModeEnv) != "1" {
		t.Skip("lock-holder subprocess only")
	}
	dir := os.Getenv(holderDirEnv)
	lock, err := tryLockDir(dir, os.Getenv(holderTokenEnv))
	if err != nil {
		os.Exit(3)
	}
	defer lock.Release()
	if os.Getenv(holderStubEnv) == "1" {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM) // swallow it and keep holding
	}
	os.WriteFile(os.Getenv(holderReadyEnv), []byte(strconv.Itoa(os.Getpid())), 0o644)
	for {
		time.Sleep(time.Hour)
	}
}

// lockHolder is a subprocess holding dir's lock, announced by its ready file.
type lockHolder struct {
	cmd *exec.Cmd
	dir string
}

// startLockHolder starts a holder process and waits for it to take the lock.
// dir must already exist. It also writes the holder's pidfile, so a takeover
// finds both records a serving instance leaves behind.
func startLockHolder(t *testing.T, dir, token string, ignoreTerm bool) *lockHolder {
	t.Helper()
	ready := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockHolderProcess$")
	cmd.Env = append(os.Environ(),
		holderModeEnv+"=1", holderDirEnv+"="+dir, holderTokenEnv+"="+token,
		holderReadyEnv+"="+ready, holderStubEnv+"="+boolEnv(ignoreTerm))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	h := &lockHolder{cmd: cmd, dir: dir}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	deadline := time.Now().Add(10 * time.Second)
	for !fileExists(ready) {
		if time.Now().After(deadline) {
			t.Fatal("lock holder never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	os.WriteFile(filepath.Join(dir, pidfileName),
		[]byte(strconv.Itoa(cmd.Process.Pid)+"\n"+token+"\n"), 0o644)
	return h
}

func (h *lockHolder) pid() int { return h.cmd.Process.Pid }

func boolEnv(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// A directory nobody serves is taken at once, and any stale cast files in it
// are swept before the caller gets to pack.
func TestAcquireCastDirFreshSweeps(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"index.m3u8", "seg_0000.m4s", "init.mp4"} {
		os.WriteFile(filepath.Join(dir, f), []byte("#EXTM3U\n"), 0o644)
	}
	lock, err := AcquireCastDir(dir, "cast-a", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	for _, f := range []string{"index.m3u8", "seg_0000.m4s", "init.mp4"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
			t.Errorf("%s survived the takeover sweep", f)
		}
	}
}

// A predecessor that holds the directory and will not let go must fail the
// takeover loudly, and leave the predecessor's files and pidfile untouched:
// never fall through to cleaning and packing on top of a live writer.
func TestAcquireCastDirFailsWhenPredecessorIgnoresStop(t *testing.T) {
	dir := t.TempDir()
	startLockHolder(t, dir, "cast-old", true) // ignores SIGTERM
	os.WriteFile(filepath.Join(dir, "index.m3u8"), []byte("#EXTM3U\n"), 0o644)

	start := time.Now()
	lock, err := AcquireCastDir(dir, "cast-new", 400*time.Millisecond)
	if err == nil {
		lock.Release()
		t.Fatal("takeover succeeded even though the predecessor never exited")
	}
	if elapsed := time.Since(start); elapsed < 350*time.Millisecond {
		t.Fatalf("takeover gave up after %s; it must wait out the grace first", elapsed)
	}
	if !strings.Contains(err.Error(), "did not stop") {
		t.Fatalf("unhelpful takeover error: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "index.m3u8")); statErr != nil {
		t.Fatal("a failed takeover swept the predecessor's files")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "helper.pid")); statErr != nil {
		t.Fatal("a failed takeover removed the predecessor's pidfile")
	}
}

// A live predecessor that honours SIGTERM hands the directory over, and the
// takeover sweeps only after that process is really gone.
func TestAcquireCastDirWaitsForPredecessorToExit(t *testing.T) {
	dir := t.TempDir()
	startLockHolder(t, dir, "cast-old", false)
	os.WriteFile(filepath.Join(dir, "index.m3u8"), []byte("#EXTM3U\n"), 0o644)

	lock, err := AcquireCastDir(dir, "cast-new", 5*time.Second)
	if err != nil {
		t.Fatalf("takeover failed: %v", err)
	}
	defer lock.Release()
	if _, err := os.Stat(filepath.Join(dir, "index.m3u8")); err == nil {
		t.Fatal("takeover did not sweep the predecessor's files")
	}
	if _, err := os.Stat(filepath.Join(dir, "helper.pid")); err == nil {
		t.Fatal("takeover left the predecessor's pidfile behind")
	}
}

// `stop` carrying the serving cast's token stops it and sweeps.
func TestStopCastStopsMatchingToken(t *testing.T) {
	dir := t.TempDir()
	startLockHolder(t, dir, "cast-a", false)
	os.WriteFile(filepath.Join(dir, "index.m3u8"), []byte("#EXTM3U\n"), 0o644)

	if err := StopCast(dir, "cast-a", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "index.m3u8")); err == nil {
		t.Fatal("stop did not sweep the stopped cast's files")
	}
}

// `stop` aimed at a cast that has already been replaced must do nothing: not
// kill the new instance, not delete the segments it is serving.
func TestStopCastIgnoresNewerCast(t *testing.T) {
	dir := t.TempDir()
	h := startLockHolder(t, dir, "cast-new", true) // swallow TERM, so only a real kill ends it
	os.WriteFile(filepath.Join(dir, "index.m3u8"), []byte("#EXTM3U\n"), 0o644)

	if err := StopCast(dir, "cast-old", 300*time.Millisecond); err != nil {
		t.Fatalf("a stale stop must be a no-op, not an error: %v", err)
	}
	if !processAlive(h.pid()) {
		t.Fatal("a stale stop killed the newer cast's instance")
	}
	if _, err := os.Stat(filepath.Join(dir, "index.m3u8")); err != nil {
		t.Fatal("a stale stop swept the newer cast's files")
	}
	if _, err := os.Stat(filepath.Join(dir, "helper.pid")); err != nil {
		t.Fatal("a stale stop removed the newer cast's pidfile")
	}
}
