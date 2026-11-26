package transcoder

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseProgressLine(t *testing.T) {
	// ffprobe reports the format duration as a number of seconds.
	const durationSec = 40.666667

	t.Run("Should parse a statistics line", func(t *testing.T) {
		line := "frame=   46 fps= 18 q=-1.0 Lsize=      76KiB time=00:00:02.34 bitrate= 265.2kbits/s speed=0.909x"

		p, ok := parseProgressLine(line, durationSec)

		require.True(t, ok)
		require.Equal(t, "46", p.FramesProcessed)
		require.Equal(t, "00:00:02.34", p.CurrentTime)
		require.Equal(t, "265.2kbits/s", p.CurrentBitrate)
		require.Equal(t, "0.909x", p.Speed)
		require.Greater(t, p.Progress, 0.0)
	})

	t.Run("Should ignore lines that are not statistics", func(t *testing.T) {
		_, ok := parseProgressLine("Stream #0:0: Video: h263, yuv420p", durationSec)
		require.False(t, ok)
	})

	t.Run("Should not panic when the media file is unknown", func(t *testing.T) {
		p, ok := parseProgressLine("frame=1 time=00:00:01.00 bitrate= 1.0kbits/s", 0)
		require.True(t, ok)
		require.Equal(t, 0.0, p.Progress)
	})
}

// TestScanStderrDropsLinesAfterStop verifies that lines parsed from the pipe
// after the stop signal fired never reach the consumer.
func TestScanStderrDropsLinesAfterStop(t *testing.T) {
	line := func(n string) string {
		return "frame=" + n + " time=00:00:0" + n + ".00 bitrate= 1.0kbits/s\r"
	}
	src := strings.NewReader(line("1") + line("2") + line("3") + line("4"))

	stop := make(chan struct{})
	out := make(chan Progress)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(out)
		scanStderr(out, io.LimitReader(src, 1<<20), 0, stop)
	}()

	// Consume one value, then stop. The reader may already have buffered
	// more lines; none of them may be published afterwards.
	p := <-out
	require.Equal(t, "1", p.FramesProcessed)
	close(stop)

	var extra []Progress
	for value := range out {
		extra = append(extra, value)
	}
	<-done

	require.Empty(t, extra, "progress must not be published after stop")
}

// TestScanStderrClosesChannelAtEOF makes sure the reader terminates on its
// own when the pipe is exhausted without a stop request.
func TestScanStderrClosesChannelAtEOF(t *testing.T) {
	src := strings.NewReader("frame=7 time=00:00:07.00 bitrate= 1.0kbits/s\r")
	out := make(chan Progress)

	go func() {
		defer close(out)
		scanStderr(out, src, 0, nil)
	}()

	var values []Progress
	for value := range out {
		values = append(values, value)
	}
	require.Len(t, values, 1)
	require.Equal(t, "7", values[0].FramesProcessed)
}

// TestStopWithoutRunIsSafe covers the "never started" case: Stop must return
// immediately and must not panic.
func TestStopWithoutRunIsSafe(t *testing.T) {
	ts := &Transcoder{}
	done := make(chan error, 1)
	go func() { done <- ts.Stop() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked without a run")
	}
}

// TestStopAfterFinishIsSafe covers the "already finished" case: a second Stop
// must return immediately without tearing anything down again.
func TestStopAfterFinishIsSafe(t *testing.T) {
	r := &run{
		stopCh:     make(chan struct{}),
		finishedCh: make(chan struct{}),
		done:       make(chan error, 1),
	}
	close(r.finishedCh)
	r.finished = true

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		r.stop()
		r.stop()
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked on an already finished run")
	}
	require.True(t, r.finished)
}
