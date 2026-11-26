// Command fakeffmpeg is a controllable stand-in for FFmpeg used by the
// end-to-end tests. Its behaviour is selected by marker arguments embedded in
// the request's command list, so tests can drive success, failure, slow,
// hanging and orphaned-worker scenarios without a real media toolchain.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const defaultPayloadSize = 256

func main() {
	args := os.Args[1:]
	joined := strings.Join(args, " ")
	if logPath := os.Getenv("FAKE_FFMPEG_LOG"); logPath != "" {
		appendLine(logPath, fmt.Sprintf("start pid=%d %s", os.Getpid(), joined))
	}

	switch {
	case has(args, "fake-orphan-worker"):
		// Behave like a tool that spawns a worker inheriting its stdout pipe.
		// The worker keeps emitting bytes while the parent is alive, so a kill
		// of only the direct PID leaves the tree running.
		startWorker()
		for {
			beat()
			time.Sleep(50 * time.Millisecond)
		}
	case has(args, "fake-fail-empty"):
		fmt.Fprintln(os.Stderr, "Error: simulated ffmpeg failure before output")
		appendLog("exit 3")
		os.Exit(3)
	case has(args, "fake-fail"):
		writePayload(outputTarget(args), payloadSize()/2)
		fmt.Fprintln(os.Stderr, "Error: simulated ffmpeg failure")
		appendLog("exit 3")
		os.Exit(3)
	case has(args, "fake-zero"):
		appendLog("exit 0")
		os.Exit(0)
	case has(args, "fake-worker"):
		for {
			beat()
			time.Sleep(50 * time.Millisecond)
		}
	case has(args, "fake-slow"):
		payload := payloadSize()
		chunk := 10
		for written := 0; written < payload; written += chunk {
			writePayload(outputTarget(args), min(chunk, payload-written))
			beat()
			time.Sleep(50 * time.Millisecond)
		}
		appendLog("exit 0")
		os.Exit(0)
	case has(args, "fake-hang"):
		for {
			beat()
			time.Sleep(50 * time.Millisecond)
		}
	default:
		writePayload(outputTarget(args), payloadSize())
		appendLog("exit 0")
		os.Exit(0)
	}
}

// startWorker launches a detached copy of itself that keeps writing to the
// inherited stdout descriptor and its own heartbeat until killed.
func startWorker() {
	exe, err := os.Executable()
	if err != nil {
		return
	}
	args := []string{"-i", "input.txt", "fake-worker"}
	cmd := execCommand(exe, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return
	}
	// Leave the worker running after this process exits.
	setOrphan(cmd)
}

func beat() {
	path := os.Getenv("FAKE_FFMPEG_HEARTBEAT")
	if path == "" {
		return
	}
	appendLine(path, "beat")
}

func appendLine(path, line string) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	fmt.Fprintln(f, line)
	f.Close()
}

func appendLog(line string) {
	if path := os.Getenv("FAKE_FFMPEG_LOG"); path != "" {
		appendLine(path, line)
	}
}

func has(args []string, marker string) bool {
	for _, arg := range args {
		if arg == marker {
			return true
		}
	}
	return false
}

// outputTarget returns the last argument unless the command writes to stdout.
func outputTarget(args []string) string {
	if len(args) == 0 {
		return ""
	}
	last := args[len(args)-1]
	if last == "pipe:1" || last == "-" {
		return ""
	}
	return last
}

func payloadSize() int {
	if value := os.Getenv("FAKE_FFMPEG_SIZE"); value != "" {
		if size, err := strconv.Atoi(value); err == nil && size > 0 {
			return size
		}
	}
	return defaultPayloadSize
}

func writePayload(target string, size int) {
	payload := []byte(strings.Repeat("M", size))
	if target == "" {
		os.Stdout.Write(payload)
		return
	}
	path := target
	if !filepath.IsAbs(path) {
		path = filepath.Join(os.Getenv("FAKE_FFMPEG_DIR"), target)
	}
	os.MkdirAll(filepath.Dir(path), 0755)
	existing, _ := os.ReadFile(path)
	os.WriteFile(path, append(existing, payload...), 0644)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
