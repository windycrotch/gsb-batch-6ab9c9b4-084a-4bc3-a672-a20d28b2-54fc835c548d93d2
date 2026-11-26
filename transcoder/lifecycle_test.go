package transcoder

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xfrr/goffmpeg/pkg/duration"
)

const (
	fixturePath = "../e2e/fixtures/input.3gp"

	// progressTimeout bounds how long a progress value may take to arrive.
	progressTimeout = 30 * time.Second
	// closeTimeout bounds how long a progress channel may take to close
	// after a stop request.
	closeTimeout = 10 * time.Second
)

func newTranscoder(t *testing.T, output string) *Transcoder {
	t.Helper()

	require.FileExists(t, fixturePath, "the repository 3gp fixture is required for end-to-end checks")

	trans := new(Transcoder)
	require.NoError(t, trans.Initialize(fixturePath, output))
	return trans
}

func tempOutput(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

// slowDown makes ffmpeg read the input at real time and encode quickly, so a
// full-length encode lasts long enough to be interrupted mid-flight.
func slowDown(trans *Transcoder) {
	trans.MediaFile().SetNativeFramerateInput(true)
	trans.MediaFile().SetVideoCodec("libx264")
	trans.MediaFile().SetPreset("ultrafast")
}

// TestRunStopClosesProgressChannel drives a real ffmpeg encode, stops it
// midway, and asserts that the progress channel terminates promptly and that
// nothing is published after the stop.
func TestRunStopClosesProgressChannel(t *testing.T) {
	trans := newTranscoder(t, tempOutput(t, "stopped.mp4"))
	slowDown(trans)

	done := trans.Run(true)
	progress := trans.Output()

	// Wait for real progress so the stop lands in the middle of the encode.
	first := receiveWithTimeout(t, progress, "initial progress")
	require.NotEmpty(t, first.FramesProcessed)

	start := time.Now()
	require.NoError(t, trans.Stop())
	require.Less(t, time.Since(start), closeTimeout, "Stop must return promptly")

	// Nothing more may be delivered once the stop has been requested.
	var closed bool
	late := drainChannel(t, progress, closeTimeout, &closed)
	require.True(t, closed, "progress channel must be closed after stop")
	require.Empty(t, late, "no progress may be published after the stop request")

	select {
	case err := <-done:
		require.NoError(t, err, "a user stop is a clean outcome")
	case <-time.After(closeTimeout):
		t.Fatal("run did not finish after Stop")
	}
}

// TestStopBeforeStartIsSafe covers calling Stop on a transcoder that never
// ran.
func TestStopBeforeStartIsSafe(t *testing.T) {
	trans := newTranscoder(t, tempOutput(t, "never.mp4"))

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		require.NoError(t, trans.Stop())
		require.NoError(t, trans.Stop())
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked before the transcoder was started")
	}
}

// TestStopAfterCompletionIsSafe covers calling Stop once the encode has
// finished on its own.
func TestStopAfterCompletionIsSafe(t *testing.T) {
	output := tempOutput(t, "complete.mp4")
	trans := newTranscoder(t, output)
	// Only a few frames, so the encode completes almost immediately.
	trans.MediaFile().SetVframes(15)
	trans.MediaFile().SetVideoCodec("libx264")
	trans.MediaFile().SetPreset("ultrafast")

	done := trans.Run(true)
	progress := trans.Output()
	var closed bool
	values := drainChannel(t, progress, progressTimeout, &closed)
	require.True(t, closed)
	require.NotEmpty(t, values)
	require.NoError(t, <-done)
	require.FileExists(t, output)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		require.NoError(t, trans.Stop())
		require.NoError(t, trans.Stop())
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked after the run finished")
	}
}

// TestRestartProgressIsIsolated starts the same transcoder twice. The first
// run is stopped midway; the second run must only surface its own progress.
//
// -itsoffset shifts the presentation timestamps of the first run by 20s, so
// the CurrentTime values of the two runs are disjoint. If any late line from
// the first task leaked into the second run, the second run would report a
// timestamp in the first run's window.
func TestRestartProgressIsIsolated(t *testing.T) {
	const firstRunOffset = 20.0

	trans := newTranscoder(t, tempOutput(t, "first.mp4"))
	slowDown(trans)
	trans.MediaFile().SetInputInitialOffset("00:00:20.000")

	done1 := trans.Run(true)
	progress1 := trans.Output()

	first := receiveWithTimeout(t, progress1, "first run progress")
	firstSecs := duration.DurToSec(first.CurrentTime)
	require.GreaterOrEqual(t, firstSecs, firstRunOffset,
		"first run should report offset timestamps, got %q", first.CurrentTime)

	require.NoError(t, trans.Stop())
	var closed bool
	drainChannel(t, progress1, closeTimeout, &closed)
	require.True(t, closed, "first run's progress channel must close on stop")
	require.NoError(t, <-done1)

	// Restart the same transcoder with a fresh output and no offset. The
	// second run's progress starts near zero and must never jump into the
	// offset window of the stopped first run.
	require.NoError(t, trans.SetOutputPath(tempOutput(t, "second.mp4")))
	trans.MediaFile().SetInputInitialOffset("")
	slowDown(trans)

	done2 := trans.Run(true)
	progress2 := trans.Output()

	seen := 0
	deadline := time.After(progressTimeout)
	for {
		select {
		case p, ok := <-progress2:
			if !ok {
				// Encode finished before we could stop it; still a valid
				// isolation result as long as no offset timestamp showed.
				require.NoError(t, <-done2)
				require.GreaterOrEqual(t, seen, 1)
				return
			}
			seen++
			require.Less(t, duration.DurToSec(p.CurrentTime), firstRunOffset,
				"second run reported a timestamp from the stopped first run: %+v", p)
			if seen >= 2 {
				require.NoError(t, trans.Stop())
				var c bool
				rest := drainChannel(t, progress2, closeTimeout, &c)
				require.True(t, c)
				require.Empty(t, rest, "no progress may follow the stop of the second run")
				<-done2
				return
			}
		case <-deadline:
			t.Fatal("second run produced no progress")
		}
	}
}

// TestStartFailureTearsDownCleanly points the transcoder at a binary that
// cannot be executed. The run must fail without leaking a reader: the
// progress channel closes and the error channel reports the start failure.
func TestStartFailureTearsDownCleanly(t *testing.T) {
	trans := newTranscoder(t, tempOutput(t, "missing-bin.mp4"))
	missing := filepath.Join(t.TempDir(), "definitely-not-ffmpeg")

	done := trans.runWith(missing, true)
	progress := trans.Output()

	// The channel must close rather than hang.
	var closed bool
	drainChannel(t, progress, closeTimeout, &closed)
	require.True(t, closed, "progress channel must close when ffmpeg cannot start")

	select {
	case err := <-done:
		require.Error(t, err, "a failed start must be reported")
		require.Contains(t, err.Error(), "failed start ffmpeg")
	case <-time.After(closeTimeout):
		t.Fatal("run did not report the start failure")
	}
}

// TestStopRacingStartDoesNotHang interrupts a transcoder immediately after
// Run returns, before ffmpeg has had time to produce a full statistics line.
// Stop must still finish the run within its bounded window and the progress
// channel must close.
func TestStopRacingStartDoesNotHang(t *testing.T) {
	trans := newTranscoder(t, tempOutput(t, "race.mp4"))
	slowDown(trans)

	done := trans.Run(true)
	progress := trans.Output()

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		require.NoError(t, trans.Stop())
	}()

	select {
	case <-stopped:
	case <-time.After(closeTimeout):
		t.Fatal("Stop did not return while the run was starting")
	}

	var closed bool
	drainChannel(t, progress, closeTimeout, &closed)
	require.True(t, closed, "progress channel must close for a stop during startup")

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(closeTimeout):
		t.Fatal("run did not finish after a stop during startup")
	}
}

func receiveWithTimeout(t *testing.T, ch <-chan Progress, what string) Progress {
	t.Helper()
	select {
	case p, ok := <-ch:
		require.True(t, ok, "%s: channel closed unexpectedly", what)
		return p
	case <-time.After(progressTimeout):
		t.Fatalf("%s: nothing received in time", what)
		return Progress{}
	}
}

// drainChannel consumes the channel until it closes or the timeout elapses.
func drainChannel(t *testing.T, ch <-chan Progress, timeout time.Duration, closed *bool) []Progress {
	t.Helper()
	var values []Progress
	deadline := time.After(timeout)
	for {
		select {
		case p, ok := <-ch:
			if !ok {
				*closed = true
				return values
			}
			values = append(values, p)
		case <-deadline:
			return values
		}
	}
}
