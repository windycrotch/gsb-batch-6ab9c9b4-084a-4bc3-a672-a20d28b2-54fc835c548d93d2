package dca

import (
	"testing"
	"time"
)

// TestHandleStderrLine pins down the parsing of ffmpeg's stats line, whose
// format differs between versions. It doesn't need ffmpeg.
func TestHandleStderrLine(t *testing.T) {
	cases := []struct {
		name     string
		line     string
		size     int
		duration time.Duration
		bitrate  float32
		speed    float32
	}{
		{
			name:     "modern KiB with elapsed",
			line:     "size=      95KiB time=00:00:15.06 bitrate=  51.7kbits/s speed= 141x elapsed=0:00:00.10    ",
			size:     95,
			duration: 15*time.Second + 60*time.Millisecond,
			bitrate:  51.7,
			speed:    141,
		},
		{
			name:     "legacy kB no elapsed",
			line:     "size=    1234kB time=00:01:02.50 bitrate= 128.0kbits/s speed=14.1x",
			size:     1234,
			duration: time.Minute + 2*time.Second + 500*time.Millisecond,
			bitrate:  128,
			speed:    14.1,
		},
		{
			name:     "unpadded size",
			line:     "size=95KiB time=00:00:00.20 bitrate=64.0kbits/s speed=1x",
			size:     95,
			duration: 200 * time.Millisecond,
			bitrate:  64,
			speed:    1,
		},
		{
			name:     "missing bitrate and speed",
			line:     "size=12KiB time=00:00:01.00",
			size:     12,
			duration: time.Second,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			session := newEncodeSession(StdEncodeOptions)
			session.handleStderrLine(c.line)

			stats := session.Stats()
			if stats.Size != c.size {
				t.Errorf("size = %d, expected %d", stats.Size, c.size)
			}
			if stats.Duration != c.duration {
				t.Errorf("duration = %v, expected %v", stats.Duration, c.duration)
			}
			if c.bitrate != 0 && stats.Bitrate != c.bitrate {
				t.Errorf("bitrate = %v, expected %v", stats.Bitrate, c.bitrate)
			}
			if c.speed != 0 && stats.Speed != c.speed {
				t.Errorf("speed = %v, expected %v", stats.Speed, c.speed)
			}
		})
	}
}

// TestHandleStderrLineIgnoresNoise makes sure non-stats output and the bare
// "size=" line ffmpeg prints on shutdown don't clobber the last real stats.
func TestHandleStderrLineIgnoresNoise(t *testing.T) {
	session := newEncodeSession(StdEncodeOptions)

	session.handleStderrLine("Input #0, ogg, from 'testaudio.ogg':")
	session.handleStderrLine("size=")
	if stats := session.Stats(); stats.Size != 0 || stats.Duration != 0 {
		t.Errorf("noise produced stats: %+v", stats)
	}

	session.handleStderrLine("size=42KiB time=00:00:10.00 bitrate=1kbits/s speed=2x")
	session.handleStderrLine("size=")

	stats := session.Stats()
	if stats.Size != 42 {
		t.Errorf("shutdown line clobbered the stats: %+v", stats)
	}
}

// TestValidateOptions checks the newly guarded option.
func TestValidateOptions(t *testing.T) {
	opts := *StdEncodeOptions
	opts.BufferedFrames = -1
	if err := opts.Validate(); err == nil {
		t.Error("negative BufferedFrames was accepted")
	}
}

// TestWriteOpusFrameStopped checks that a stopped session never blocks a
// writer and never panics, even though the channel is still open.
func TestWriteOpusFrameStopped(t *testing.T) {
	opts := *StdEncodeOptions
	opts.BufferedFrames = 0 // unbuffered: a send would block forever
	session := newEncodeSession(&opts)

	if err := session.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- session.writeOpusFrame([]byte("frame"))
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("writeOpusFrame succeeded after Stop")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writeOpusFrame blocked after Stop")
	}

	// The run goroutine was never started here, so complete the lifecycle.
	close(session.frameChannel)
	close(session.doneCh)
}
