package ffmpeg

import (
	"testing"
)

func TestFFProbeRun(t *testing.T) {
	requireFFmpeg(t, "ffprobe")
	ffprobe := &FFProbe{}
	probe, err := ffprobe.Run(testFile)
	if err != nil {
		t.Fatal(err)
	}

	if len(probe.Streams) != 2 {
		t.Fatal()
	}

	if probe.Streams[0].Index != 0 {
		t.Error()
	}

	if probe.Streams[0].Width != 1280 {
		t.Error()
	}

	if probe.Streams[0].Height != 534 {
		t.Error()
	}
}
