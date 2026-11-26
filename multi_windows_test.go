//go:build windows
// +build windows

package hls

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMultiWithBatchFake proves a .bat fake ffmpeg (a natural Windows test
// double) works with injection, exit-status reporting and cancellation.
func TestMultiWithBatchFake(t *testing.T) {
	dir := t.TempDir()
	bat := filepath.Join(dir, "fake-ffmpeg.bat")
	// The parse loop leaves OUT at the final argument, which getOptions
	// defines as the variant playlist path.
	script := "@echo off\r\n" +
		"setlocal enabledelayedexpansion\r\n" +
		"set \"OUT=\"\r\n" +
		":parse\r\n" +
		"if \"%~1\"==\"\" goto parsed\r\n" +
		"set \"OUT=%~1\"\r\n" +
		"shift\r\n" +
		"goto parse\r\n" +
		":parsed\r\n" +
		"for %%F in (\"%OUT%\") do (\r\n" +
		"  set \"RES=%%~nF\"\r\n" +
		"  set \"OUTDIR=%%~dpF\"\r\n" +
		")\r\n" +
		"if \"%RES%\"==\"%FAKE_SLEEP_RES%\" (\r\n" +
		"  echo started> \"%FAKE_STARTED_FILE%.started-%RES%\"\r\n" +
		"  ping -n 60 127.0.0.1 >nul\r\n" +
		")\r\n" +
		"echo fake-bat-segment> \"%OUTDIR%%RES%_000.ts\"\r\n" +
		"echo #EXTM3U> \"%OUT%\"\r\n" +
		"if \"%RES%\"==\"%FAKE_FAIL_RES%\" exit /b 1\r\n" +
		"exit /b 0\r\n"

	if err := os.WriteFile(bat, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(dir, "out")
	if err := GenerateHLSMulti(bat, "in.mp4", target, []string{"360p", "720p"}); err != nil {
		t.Fatalf("batch fake success run: %v", err)
	}
	if urls := masterURLs(t, filepath.Join(target, "playlist.m3u8")); strings.Join(urls, ",") != "360p.m3u8,720p.m3u8" {
		t.Fatalf("unexpected master urls: %v", urls)
	}
	if _, err := os.Stat(filepath.Join(target, "720p_000.ts")); err != nil {
		t.Fatalf("batch-produced segment missing: %v", err)
	}

	// Failure via batch exit code.
	failTarget := filepath.Join(dir, "failout")
	t.Setenv("FAKE_FAIL_RES", "720p")
	err := GenerateHLSMulti(bat, "in.mp4", failTarget, []string{"360p", "720p"})
	if err == nil || !strings.Contains(err.Error(), "720p") {
		t.Fatalf("expected 720p failure, got %v", err)
	}
	if entries, _ := os.ReadDir(failTarget); len(entries) != 0 {
		t.Fatalf("failed batch run leaked artifacts: %v", entries)
	}

	// Cancellation kills cmd/ping tree and cleans up.
	cancelTarget := filepath.Join(dir, "cancelout")
	started := filepath.Join(dir, "bat-started")
	t.Setenv("FAKE_SLEEP_RES", "360p")
	t.Setenv("FAKE_STARTED_FILE", started)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- GenerateHLSMultiContext(ctx, bat, "in.mp4", cancelTarget, []string{"360p"}) }()

	deadline := time.Now().Add(15 * time.Second)
	marker := started + ".started-360p"
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("batch fake never started sleeping: %v", err)
	}
	cancel()
	select {
	case err := <-errCh:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled wrap, got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("batch cancellation timed out")
	}
}
