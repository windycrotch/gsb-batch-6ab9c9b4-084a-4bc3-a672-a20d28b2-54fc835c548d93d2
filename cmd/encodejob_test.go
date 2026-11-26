package cmd

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/alfg/ffmpegd/ffmpeg"
)

func TestMain(m *testing.M) {
	// The fake-ffmpeg integration test points ffmpeg.Exec at this binary, so a
	// child process runs the fake encoder instead of the test suite.
	if os.Getenv("FFMPEGD_CMD_FAKE_FFMPEG") == "1" {
		runFakeFFmpegChild()
		return
	}
	os.Exit(m.Run())
}

// runFakeFFmpegChild behaves like an ffmpeg that writes progress and then runs
// until it is killed. It writes one progress line right away, which both marks
// it as started and gives the client something to observe.
func runFakeFFmpegChild() {
	os.Stdout.WriteString("frame=3\nfps=25.0\nout_time_us=300000\nspeed=1.5x\n")
	os.Stdout.Sync()
	time.Sleep(time.Minute)
}

// useFakeFFmpegProcess makes ffmpeg.Exec launch this binary as a fake encoder.
func useFakeFFmpegProcess(t *testing.T) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locating test binary: %v", err)
	}
	old := ffmpeg.Exec
	ffmpeg.Exec = func(args ...string) *exec.Cmd {
		c := exec.Command(self)
		c.Env = append(os.Environ(), "FFMPEGD_CMD_FAKE_FFMPEG=1")
		return c
	}
	t.Setenv("FFMPEGD_CMD_FAKE_FFMPEG", "1")
	t.Cleanup(func() { ffmpeg.Exec = old })
}

// useFakeProbe returns a fixed probe result without running ffprobe.
func useFakeProbe(t *testing.T) {
	t.Helper()
	old := probeInput
	probeInput = func(input string) (*ffmpeg.FFProbeResponse, error) {
		return &ffmpeg.FFProbeResponse{}, nil
	}
	t.Cleanup(func() { probeInput = old })
}

// shortProgressInterval keeps the integration test fast.
func shortProgressInterval(t *testing.T, d time.Duration) {
	t.Helper()
	old := progressInterval
	progressInterval = d
	t.Cleanup(func() { progressInterval = old })
}

const fakePayload = `{"video":{"codec":"libx264","crf":23}}`

// TestEncodeJobCancelStopsRealProcess drives the production encodeJob end to
// end: real probe, real FFmpeg wrapper, real child process. Only the encoder
// executable is faked, so this covers the wiring the session tests stub out.
func TestEncodeJobCancelStopsRealProcess(t *testing.T) {
	useFakeFFmpegProcess(t)
	useFakeProbe(t)
	shortProgressInterval(t, 20*time.Millisecond)

	var mu sync.Mutex
	var statuses []Status
	report := func(st Status) {
		mu.Lock()
		statuses = append(statuses, st)
		mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- encodeJob(ctx, Message{Input: "in.mp4", Output: "out.mp4", Payload: fakePayload}, report)
	}()

	// Wait until the fake process is running and its progress has been read.
	deadline := time.After(15 * time.Second)
	for {
		mu.Lock()
		n := len(statuses)
		mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("fake ffmpeg never reported progress")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, ffmpeg.ErrCancelled) {
			t.Fatalf("encodeJob = %v, want ffmpeg.ErrCancelled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("encodeJob did not return after cancellation; process was not killed")
	}
}

// TestEncodeJobCancelBeforeProcessStarts covers the window between dequeue and
// the process launching: a cancel there must return ErrCancelled without
// starting ffmpeg, and must not leak a process.
func TestEncodeJobCancelBeforeProcessStarts(t *testing.T) {
	startedProbe := make(chan struct{})
	releaseProbe := make(chan struct{})
	old := probeInput
	probeInput = func(input string) (*ffmpeg.FFProbeResponse, error) {
		close(startedProbe)
		<-releaseProbe
		return &ffmpeg.FFProbeResponse{}, nil
	}
	t.Cleanup(func() { probeInput = old })

	launched := make(chan struct{}, 1)
	oldExec := ffmpeg.Exec
	ffmpeg.Exec = func(args ...string) *exec.Cmd {
		launched <- struct{}{}
		return exec.Command("this-should-never-run")
	}
	t.Cleanup(func() { ffmpeg.Exec = oldExec })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- encodeJob(ctx, Message{Input: "in.mp4", Output: "out.mp4", Payload: fakePayload}, func(Status) {})
	}()

	<-startedProbe
	cancel()
	close(releaseProbe) // the real probe cannot be interrupted; simulate it returning late

	select {
	case err := <-done:
		if !errors.Is(err, ffmpeg.ErrCancelled) {
			t.Fatalf("encodeJob = %v, want ffmpeg.ErrCancelled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("encodeJob did not return after an early cancellation")
	}

	select {
	case <-launched:
		t.Fatal("ffmpeg was started despite cancellation before it began")
	default:
	}
}
