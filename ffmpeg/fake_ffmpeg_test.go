package ffmpeg

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestMain lets the test binary act as a controllable fake ffmpeg. When the
// helper environment variable is set, the process runs the fake encoder instead
// of the test suite, so cancellation and progress can be driven without a real
// encode and without depending on timing.
func TestMain(m *testing.M) {
	if mode := os.Getenv("FFMPEGD_FAKE_FFMPEG"); mode != "" {
		runFakeFFmpeg(mode)
		return
	}
	os.Exit(m.Run())
}

const (
	fakeModeSlow      = "slow"      // writes progress forever until killed
	fakeModeQuick     = "quick"     // writes progress, then exits successfully
	fakeModeFail      = "fail"      // writes to stderr, then exits non-zero
	fakeModeSuccessAt = "successAt" // exits successfully after a delay
)

func runFakeFFmpeg(mode string) {
	switch mode {
	case fakeModeSlow:
		fmt.Println("frame=1")
		fmt.Println("fps=25.0")
		fmt.Println("out_time_us=500000")
		fmt.Println("speed=1.0x")
		os.Stdout.Sync()
		// Stay alive until the test kills us.
		time.Sleep(time.Hour)
	case fakeModeQuick:
		fmt.Println("frame=10")
		fmt.Println("out_time_us=1000000")
		fmt.Println("speed=2.0x")
		os.Stdout.Sync()
		os.Exit(0)
	case fakeModeFail:
		fmt.Fprintln(os.Stderr, "fake ffmpeg: invalid option")
		os.Exit(1)
	case fakeModeSuccessAt:
		// Emit progress, then finish successfully after a short delay so the
		// test can cancel right around the exit.
		fmt.Println("frame=5")
		fmt.Println("out_time_us=500000")
		os.Stdout.Sync()
		ms, _ := strconv.Atoi(os.Getenv("FFMPEGD_FAKE_DELAY_MS"))
		time.Sleep(time.Duration(ms) * time.Millisecond)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown fake ffmpeg mode %q\n", mode)
		os.Exit(2)
	}
}

// useFakeFFmpeg points the encoder at this test binary running the fake mode.
func useFakeFFmpeg(t *testing.T, mode string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locating test binary: %v", err)
	}
	old := Exec
	Exec = func(args ...string) *exec.Cmd {
		c := exec.Command(self, "-test.run=TestMain")
		c.Args = append(c.Args, args...)
		return c
	}
	t.Setenv("FFMPEGD_FAKE_FFMPEG", mode)
	t.Cleanup(func() { Exec = old })
}

// singlePassEncode builds an encode whose only pass runs the fake ffmpeg with
// whatever arguments it was given.
func singlePassEncode(t *testing.T) *Encode {
	t.Helper()
	e, err := NewEncode("in.mp4", "out.mp4", testPayload)
	if err != nil {
		t.Fatalf("NewEncode: %v", err)
	}
	t.Cleanup(e.Close)
	return e
}

func TestExitErrorFinishedJobStaysFinished(t *testing.T) {
	// A successful exit is a completion even if a cancel arrived at the last
	// instant. This is the regression that let a cancelled job also report 100%.
	if err := exitError(nil, true, ""); err != nil {
		t.Fatalf("exitError(success, cancelled) = %v, want nil", err)
	}
}

func TestExitErrorCancelledProcess(t *testing.T) {
	if err := exitError(errors.New("signal: killed"), true, ""); !errors.Is(err, ErrCancelled) {
		t.Fatalf("exitError(failure, cancelled) = %v, want ErrCancelled", err)
	}
}

func TestExitErrorFailureUsesStderr(t *testing.T) {
	err := exitError(errors.New("exit status 1"), false, "  boom  \n")
	if err == nil || err.Error() != "boom" {
		t.Fatalf("exitError(failure, not cancelled, stderr) = %v, want %q", err, "boom")
	}
}

func TestRunEncodeProgressThenSuccess(t *testing.T) {
	useFakeFFmpeg(t, fakeModeQuick)
	f := &FFmpeg{}
	if err := f.RunEncode(singlePassEncode(t)); err != nil {
		t.Fatalf("RunEncode: %v", err)
	}
	p := f.Progress()
	if p.Pass != 1 || p.Passes != 1 {
		t.Fatalf("progress pass = %d/%d, want 1/1", p.Pass, p.Passes)
	}
	if p.Frame != 10 || p.OutTimeUS != 1000000 || p.Speed != "2.0x" {
		t.Fatalf("progress = %+v, want the fake's last values", p)
	}
}

func TestRunEncodeCancelStopsProcess(t *testing.T) {
	useFakeFFmpeg(t, fakeModeSlow)
	f := &FFmpeg{}

	// Wait until the fake is actually running and reporting progress, so this
	// tests cancelling a live process rather than a not-yet-started one.
	done := make(chan error, 1)
	go func() { done <- f.RunEncode(singlePassEncode(t)) }()

	deadline := time.After(10 * time.Second)
	for f.Progress().Frame == 0 {
		select {
		case <-deadline:
			t.Fatal("fake ffmpeg never reported progress")
		case <-time.After(5 * time.Millisecond):
		}
	}

	f.Cancel()

	select {
	case err := <-done:
		if !errors.Is(err, ErrCancelled) {
			t.Fatalf("RunEncode after Cancel = %v, want ErrCancelled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RunEncode did not return after Cancel; process was not killed")
	}
}

func TestRunEncodeCancelBeforeStart(t *testing.T) {
	useFakeFFmpeg(t, fakeModeSlow)
	f := &FFmpeg{}
	// Cancelling before the process exists must be remembered, so the encode
	// never starts.
	f.Cancel()
	if err := f.RunEncode(singlePassEncode(t)); !errors.Is(err, ErrCancelled) {
		t.Fatalf("RunEncode after early Cancel = %v, want ErrCancelled", err)
	}
}

func TestRunEncodeCancelRacingSuccessStaysSuccess(t *testing.T) {
	useFakeFFmpeg(t, fakeModeSuccessAt)
	t.Setenv("FFMPEGD_FAKE_DELAY_MS", "100")
	f := &FFmpeg{}

	done := make(chan error, 1)
	go func() { done <- f.RunEncode(singlePassEncode(t)) }()

	// Cancel while the fake is exiting successfully. Whether the kill lands or
	// the process already finished, the job must not end up both finished and
	// cancelled: a successful exit wins.
	time.Sleep(80 * time.Millisecond)
	f.Cancel()

	err := <-done
	if err != nil && !errors.Is(err, ErrCancelled) {
		t.Fatalf("RunEncode = %v, want nil or ErrCancelled", err)
	}
}

func TestRunEncodeFailureSurfacesStderr(t *testing.T) {
	useFakeFFmpeg(t, fakeModeFail)
	f := &FFmpeg{}
	err := f.RunEncode(singlePassEncode(t))
	if err == nil {
		t.Fatal("RunEncode of a failing encode returned nil")
	}
	if errors.Is(err, ErrCancelled) {
		t.Fatalf("failure reported as cancellation: %v", err)
	}
	if got, want := err.Error(), "fake ffmpeg: invalid option"; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

// TestTwoPassCancelStopsBeforeSecondPass checks that cancelling during the first
// pass of a two-pass encode does not launch the second pass.
func TestTwoPassCancelStopsBeforeSecondPass(t *testing.T) {
	useFakeFFmpeg(t, fakeModeSlow)

	var launches int
	var mu sync.Mutex
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	old := Exec
	t.Cleanup(func() { Exec = old })
	Exec = func(args ...string) *exec.Cmd {
		mu.Lock()
		launches++
		mu.Unlock()
		c := exec.Command(self, "-test.run=TestMain")
		c.Args = append(c.Args, args...)
		return c
	}

	// A two-pass x264 encode produces two passes.
	e, err := NewEncode("in.mp4", "out.mp4",
		`{"video":{"codec":"libx264","pass":"2","bitrate":"2M"},"audio":{"codec":"aac"}}`)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if n := len(e.Passes()); n != 2 {
		t.Fatalf("encode has %d passes, want 2", n)
	}

	f := &FFmpeg{}
	done := make(chan error, 1)
	go func() { done <- f.RunEncode(e) }()

	deadline := time.After(10 * time.Second)
	for f.Progress().Frame == 0 {
		select {
		case <-deadline:
			t.Fatal("first pass never reported progress")
		case <-time.After(5 * time.Millisecond):
		}
	}
	f.Cancel()

	select {
	case err := <-done:
		if !errors.Is(err, ErrCancelled) {
			t.Fatalf("RunEncode = %v, want ErrCancelled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RunEncode did not return after cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	if launches != 1 {
		t.Fatalf("ffmpeg launched %d times, want 1 (second pass must not start)", launches)
	}
}
