package spireav_test

import (
	"context"
	"os/exec"
	"testing"

	"github.com/spiretechnology/spireav"
	"github.com/stretchr/testify/require"
)

func overrideFfmpegPath(path string) func() {
	original := spireav.FfmpegPath
	spireav.FfmpegPath = path
	return func() {
		spireav.FfmpegPath = original
	}
}

// requireFfmpeg skips tests that need a real FFmpeg binary when none is installed, so
// that the suite can run locally without it.
func requireFfmpeg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath(spireav.FfmpegPath); err != nil {
		t.Skipf("skipping: ffmpeg binary not available: %v", err)
	}
}

func TestRunnerErrors(t *testing.T) {
	t.Run("invalid args", func(t *testing.T) {
		requireFfmpeg(t)
		runner := spireav.NewRunner(spireav.RunnableArgs{"several", "bad", "args"})
		err := runner.Run(context.Background())
		require.Error(t, err, "expected error when running process with invalid args")
		require.ErrorContains(t, err, "Invalid argument")
	})
	t.Run("ffmpeg not in path", func(t *testing.T) {
		defer overrideFfmpegPath("something-that-does-not-exist")()
		runner := spireav.NewRunner(spireav.RunnableArgs{"-i", "something"})
		err := runner.Run(context.Background())
		require.Error(t, err, "expected error when path is missing ffmpeg binary")
		require.ErrorContains(t, err, "starting ffmpeg process: exec: \"something-that-does-not-exist\": executable file not found")
	})
}
