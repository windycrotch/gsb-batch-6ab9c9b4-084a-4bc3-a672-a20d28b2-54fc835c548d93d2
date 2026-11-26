package spireav_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spiretechnology/spireav"
	"github.com/stretchr/testify/require"
)

// fakeRunnable makes the runner execute the test binary itself in the given fake mode.
// The mode and its extra arguments are passed straight through as FFmpeg's argv.
type fakeRunnable struct {
	args []string
}

func (f fakeRunnable) RunnableArgs() ([]string, error) {
	return f.args, nil
}

// useFakeFfmpeg points the runner at the test binary and configures the fake mode.
func useFakeFfmpeg(t *testing.T, mode string, extraArgs ...string) {
	t.Helper()
	if os.Getenv(fakeFfmpegEnv) == "1" {
		t.Fatal("tests must not run inside the fake ffmpeg process")
	}
	original := spireav.FfmpegPath
	spireav.FfmpegPath = os.Args[0]
	t.Cleanup(func() {
		spireav.FfmpegPath = original
	})
	t.Setenv(fakeFfmpegEnv, "1")
	fakeMode = append([]string{mode}, extraArgs...)
}

// fakeMode is read by the test's runner setup. Tests are not parallel, so a package
// level variable is sufficient.
var fakeMode = []string{"success"}

func newFakeRunner(t *testing.T, opts ...spireav.Option) spireav.Runner {
	t.Helper()
	return spireav.NewRunner(fakeRunnable{args: fakeMode}, opts...)
}

func TestRunSuccess(t *testing.T) {
	useFakeFfmpeg(t, "success")

	var (
		mu      sync.Mutex
		reports []spireav.Progress
	)
	runner := newFakeRunner(t, spireav.WithProgressCallback(func(p spireav.Progress) {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, p)
	}))

	require.NoError(t, runner.Run(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, reports, 3, "one callback per progress block")
	require.True(t, reports[len(reports)-1].Done, "last callback must be marked done")

	// Elapsed must be monotonically non-decreasing and match the received order.
	for i := 1; i < len(reports); i++ {
		require.GreaterOrEqual(t, reports[i].Elapsed, reports[i-1].Elapsed,
			"elapsed must not decrease (index %d)", i)
	}
	require.Equal(t, int64(0), reports[0].Frame)
	require.Equal(t, int64(1), reports[1].Frame)
	require.Equal(t, int64(2), reports[2].Frame, "reports must arrive in output order")
}

func TestRunSuccessWithoutExplicitEnd(t *testing.T) {
	useFakeFfmpeg(t, "success-no-end")

	var (
		mu      sync.Mutex
		reports []spireav.Progress
	)
	runner := newFakeRunner(t, spireav.WithProgressCallback(func(p spireav.Progress) {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, p)
	}))

	require.NoError(t, runner.Run(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, reports)
	require.True(t, reports[len(reports)-1].Done,
		"a successful run must finish with a done callback even without progress=end")
	require.Equal(t, 1, countDone(reports), "only the final callback should be marked done")
}

func TestRunFailure(t *testing.T) {
	useFakeFfmpeg(t, "failure")

	var (
		mu      sync.Mutex
		reports []spireav.Progress
	)
	runner := newFakeRunner(t, spireav.WithProgressCallback(func(p spireav.Progress) {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, p)
	}))

	err := runner.Run(context.Background())
	require.Error(t, err)

	var ffErr *spireav.FfmpegError
	require.ErrorAs(t, err, &ffErr, "error must be an *FfmpegError")
	require.Equal(t, 3, ffErr.ExitCode)
	require.Contains(t, ffErr.Logs, "Invalid argument: no such input file")

	// No callback may be marked done once the process has failed.
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, countDone(reports), "no done callback may be delivered on failure")
}

func TestRunCancel(t *testing.T) {
	dir := t.TempDir()
	readyPath := filepath.Join(dir, "ready")
	goPath := filepath.Join(dir, "go")
	useFakeFfmpeg(t, "cancel", readyPath, goPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu    sync.Mutex
		calls int
	)
	runner := newFakeRunner(t, spireav.WithProgressCallback(func(p spireav.Progress) {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(2 * time.Millisecond) // simulate a slow consumer
	}))

	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(ctx)
	}()

	// Wait until the fake process has emitted some blocks, then cancel.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	}, 5*time.Second, 5*time.Millisecond)

	cancel()
	require.NoError(t, os.WriteFile(goPath, []byte("go"), 0o600))

	select {
	case err := <-runErr:
		require.ErrorIs(t, err, context.Canceled, "Run must return the context's error")
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return promptly after the context was canceled")
	}

	// No callback may be invoked after Run returned (this also proves that no goroutine
	// is still writing callbacks).
	mu.Lock()
	callsAtReturn := calls
	mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, callsAtReturn, calls, "no progress callback may fire after Run returns")
}

func TestRunCancelBeforeAnyProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var (
		mu    sync.Mutex
		calls int
	)
	useFakeFfmpeg(t, "hang")
	runner := newFakeRunner(t, spireav.WithProgressCallback(func(p spireav.Progress) {
		mu.Lock()
		defer mu.Unlock()
		calls++
	}))

	start := time.Now()
	err := runner.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 5*time.Second, "Run must return promptly")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, calls, "no callback may fire for an already-canceled context")
}

// TestRunCancelWhileCallbackRunning verifies that Run does not return while a callback
// is still running: the callback is blocked, and Run must not return until it is
// released, and must not deliver another callback afterwards.
func TestRunCancelWhileCallbackRunning(t *testing.T) {
	useFakeFfmpeg(t, "success")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var (
		mu    sync.Mutex
		calls int
	)
	runner := newFakeRunner(t, spireav.WithProgressCallback(func(p spireav.Progress) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			entered <- struct{}{}
			<-release // block the first callback
		}
	}))

	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(ctx)
	}()

	<-entered
	cancel()

	// Run must not return while the callback is blocked.
	select {
	case err := <-runErr:
		t.Fatalf("Run returned while a callback was still running: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-runErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the callback was released")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, calls, "no callback may start after cancellation was observed")
}

func TestRunMalformedOutput(t *testing.T) {
	// Output is split into arbitrary fragments, contains unrecognized lines, and repeats
	// the progress key of an already-delivered block.
	useFakeFfmpeg(t, "malformed")

	var (
		mu      sync.Mutex
		reports []spireav.Progress
	)
	runner := newFakeRunner(t, spireav.WithProgressCallback(func(p spireav.Progress) {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, p)
	}))

	require.NoError(t, runner.Run(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, reports, 3, "each progress block must be delivered exactly once")
	require.Equal(t, int64(1), reports[0].Frame)
	require.Equal(t, int64(1), reports[1].Frame, "a fragmented block must not be misparsed")
	require.Equal(t, int64(2), reports[2].Frame)
	require.False(t, reports[0].Done)
	require.False(t, reports[1].Done)
	require.True(t, reports[2].Done, "the final block is progress=end")
	require.Equal(t, "00:00:02.000000", reports[2].OutTime)
	require.Equal(t, int64(4096), reports[2].TotalSize)

	for i := 1; i < len(reports); i++ {
		require.GreaterOrEqual(t, reports[i].Elapsed, reports[i-1].Elapsed)
	}
}

func countDone(reports []spireav.Progress) int {
	n := 0
	for _, r := range reports {
		if r.Done {
			n++
		}
	}
	return n
}
