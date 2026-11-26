package dca

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// requireFFmpeg skips a test when ffmpeg/ffprobe are not installed. The
// encoding tests need a real ffmpeg, they are not unit tests of a mock.
func requireFFmpeg(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not found in PATH, skipping encoding test", bin)
		}
	}
	if _, err := os.Stat("testaudio.ogg"); err != nil {
		t.Skipf("testaudio.ogg missing: %v", err)
	}
}

// within fails the test when fn does not return within d.
func within(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
	}
}

// TestEncode performs a complete transcode of the sample file. The metadata
// frame must be the very first frame and appear exactly once, followed by
// audio frames, and the session must end with io.EOF.
func TestEncode(t *testing.T) {
	requireFFmpeg(t)

	session, err := EncodeFile("testaudio.ogg", StdEncodeOptions)
	if err != nil {
		t.Fatal("Failed creating encoding session:", err)
	}
	defer session.Cleanup()

	metadataFrames := 0
	audioFrames := 0
	first := true

	for {
		frame, err := session.ReadFrame()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("unexpected read error: %v", err)
			}
			break
		}

		if first {
			first = false
			if !bytes.HasPrefix(frame, []byte("DCA")) {
				t.Fatalf("first frame is not a metadata frame, got prefix %q", frame[:min(4, len(frame))])
			}
		}

		if bytes.HasPrefix(frame, []byte("DCA")) {
			metadataFrames++
			if audioFrames > 0 {
				t.Fatalf("metadata frame arrived after %d audio frames", audioFrames)
			}
			continue
		}

		if len(frame) < 3 {
			t.Fatalf("audio frame too short: %d bytes", len(frame))
		}
		audioFrames++
	}

	if metadataFrames != 1 {
		t.Errorf("expected exactly 1 metadata frame, got %d", metadataFrames)
	}

	// The exact frame count depends on the ffmpeg build (the last packet is
	// padded differently between versions), so only sanity check the range.
	// 15.066s at 20ms/frame is ~753 frames.
	if audioFrames < 700 || audioFrames > 800 {
		t.Errorf("unexpected number of audio frames: %d", audioFrames)
	}

	if session.Running() {
		t.Error("session still reports running after the frame stream ended")
	}
}

// TestEncodeRawOutput checks that RawOutput suppresses the metadata frame.
func TestEncodeRawOutput(t *testing.T) {
	requireFFmpeg(t)

	options := *StdEncodeOptions
	options.RawOutput = true

	session, err := EncodeFile("testaudio.ogg", &options)
	if err != nil {
		t.Fatal("Failed creating encoding session:", err)
	}
	defer session.Cleanup()

	frame, err := session.ReadFrame()
	if err != nil {
		t.Fatalf("reading the first frame failed: %v", err)
	}

	if bytes.HasPrefix(frame, []byte("DCA")) {
		t.Fatal("RawOutput produced a metadata frame")
	}

	// The first frame must already be a framed opus packet.
	if len(frame) < 3 {
		t.Fatalf("first raw frame too short: %d", len(frame))
	}

	// And the opus frame accessor must agree.
	opus, err := session.OpusFrame()
	if err != nil {
		t.Fatalf("OpusFrame failed: %v", err)
	}
	if bytes.HasPrefix(opus, []byte("DCA")) {
		t.Error("OpusFrame returned metadata bytes")
	}
}

// TestEncodeDecodeRoundTrip encodes a whole file and decodes the produced
// stream again. Every audio frame written must come back out.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	requireFFmpeg(t)

	session, err := EncodeFile("testaudio.ogg", StdEncodeOptions)
	if err != nil {
		t.Fatal("Failed creating encoding session:", err)
	}

	var encoded bytes.Buffer
	if _, err := io.Copy(&encoded, session); err != nil {
		session.Cleanup()
		t.Fatalf("copying the encoded stream failed: %v", err)
	}
	session.Cleanup()

	decoder := NewDecoder(bytes.NewReader(encoded.Bytes()))
	if err := decoder.ReadMetadata(); err != nil {
		t.Fatalf("decoding metadata failed: %v", err)
	}
	if decoder.Metadata == nil || decoder.Metadata.Opus == nil {
		t.Fatal("metadata did not round trip")
	}
	if decoder.Metadata.Opus.Channels != StdEncodeOptions.Channels {
		t.Errorf("metadata channels = %d, expected %d", decoder.Metadata.Opus.Channels, StdEncodeOptions.Channels)
	}

	audioFrames := 0
	for {
		if _, err := decoder.OpusFrame(); err != nil {
			if err != io.EOF {
				t.Fatalf("decode error: %v", err)
			}
			break
		}
		audioFrames++
	}

	if audioFrames < 700 {
		t.Errorf("only %d audio frames decoded back", audioFrames)
	}
}

// TestEncodeMemStop starts a transcode from a pipe that never delivers data,
// so ffmpeg stays alive, then stops it. Readers blocked in ReadFrame must be
// woken up, and the session must be reaped.
func TestEncodeMemStopUnblocksReaders(t *testing.T) {
	requireFFmpeg(t)

	options := *StdEncodeOptions
	options.BufferedFrames = 1

	// This reader blocks forever, so ffmpeg never sees end of input.
	pr, pw := io.Pipe()
	defer pw.Close()
	defer pr.Close()

	session, err := EncodeMem(pr, &options)
	if err != nil {
		t.Fatal("Failed creating encoding session:", err)
	}

	var wg sync.WaitGroup
	readResults := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, err := session.ReadFrame()
				if err != nil {
					readResults <- err
					return
				}
			}
		}()
	}

	// Let the run goroutine get ffmpeg going.
	time.Sleep(300 * time.Millisecond)

	within(t, 10*time.Second, "Stop", func() {
		if err := session.Stop(); err != nil && !errors.Is(err, ErrNotRunning) {
			t.Errorf("Stop returned %v", err)
		}
	})

	// Stop must be observable as "not running" on the second call.
	if err := session.Stop(); !errors.Is(err, ErrNotRunning) {
		t.Errorf("second Stop returned %v, expected ErrNotRunning", err)
	}

	// Every reader must terminate, and with an error we can classify.
	within(t, 10*time.Second, "readers terminating", func() {
		wg.Wait()
	})
	close(readResults)
	for err := range readResults {
		if err == nil {
			t.Error("reader returned a nil error")
		}
	}

	within(t, 10*time.Second, "Cleanup", func() {
		session.Cleanup()
	})

	if session.Running() {
		t.Error("session still running after Cleanup")
	}
}

// TestStopImmediately checks that Stop right after creation cannot hang: the
// run goroutine may not have started ffmpeg yet.
func TestStopImmediately(t *testing.T) {
	requireFFmpeg(t)

	options := *StdEncodeOptions
	for i := 0; i < 5; i++ {
		session, err := EncodeMem(strings.NewReader("not audio"), &options)
		if err != nil {
			t.Fatal(err)
		}

		within(t, 5*time.Second, "immediate Stop", func() {
			_ = session.Stop()
		})
		within(t, 10*time.Second, "Cleanup after immediate Stop", func() {
			session.Cleanup()
		})
	}
}

// TestCleanupIdempotent calls Cleanup repeatedly, sequentially and
// concurrently. None of the calls may panic or block.
func TestCleanupIdempotent(t *testing.T) {
	requireFFmpeg(t)

	session, err := EncodeFile("testaudio.ogg", StdEncodeOptions)
	if err != nil {
		t.Fatal(err)
	}

	// Read a few frames so the transcode is really in flight.
	for i := 0; i < 3; i++ {
		if _, err := session.ReadFrame(); err != nil {
			t.Fatalf("reading frame %d failed: %v", i, err)
		}
	}

	within(t, 15*time.Second, "concurrent Cleanup calls", func() {
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				session.Cleanup()
			}()
		}
		wg.Wait()
	})

	// A sequential repeat must return too.
	within(t, 5*time.Second, "repeated Cleanup", func() {
		session.Cleanup()
		session.Cleanup()
	})

	if err := session.Stop(); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Stop after Cleanup returned %v, expected ErrNotRunning", err)
	}

	// Reads after the session is gone must not block either.
	within(t, 5*time.Second, "ReadFrame after Cleanup", func() {
		if _, err := session.ReadFrame(); err != io.EOF {
			t.Errorf("ReadFrame after Cleanup returned %v, expected io.EOF", err)
		}
	})
}

// TestStopDuringRead hammers Stop while a reader is consuming frames, which is
// the pattern a caller uses when changing songs mid playback.
func TestStopDuringRead(t *testing.T) {
	requireFFmpeg(t)

	for i := 0; i < 3; i++ {
		options := *StdEncodeOptions
		options.BufferedFrames = 2

		session, err := EncodeFile("testaudio.ogg", &options)
		if err != nil {
			t.Fatal(err)
		}

		readerDone := make(chan error, 1)
		go func() {
			for {
				if _, err := session.OpusFrame(); err != nil {
					readerDone <- err
					return
				}
			}
		}()

		// Stop at a point where much of the file is still to be transcoded.
		time.Sleep(50 * time.Millisecond)
		within(t, 10*time.Second, "Stop during read", func() {
			_ = session.Stop()
		})

		select {
		case err := <-readerDone:
			if err == nil {
				t.Error("reader returned nil error after Stop")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("reader still blocked after Stop")
		}

		within(t, 10*time.Second, "Cleanup", func() {
			session.Cleanup()
		})
	}
}

// TestStopWithNoReader checks the deadlock case where nobody reads frames: the
// buffer fills up, the writer blocks on the channel, and Stop/Cleanup must still
// unblock it and reap ffmpeg.
func TestStopWithNoReader(t *testing.T) {
	requireFFmpeg(t)

	options := *StdEncodeOptions
	options.BufferedFrames = 1

	session, err := EncodeFile("testaudio.ogg", &options)
	if err != nil {
		t.Fatal(err)
	}

	// Give the transcode time to fill the one-slot buffer and block.
	time.Sleep(500 * time.Millisecond)

	within(t, 10*time.Second, "Stop with a blocked writer", func() {
		if err := session.Stop(); !errors.Is(err, nil) && !errors.Is(err, ErrNotRunning) {
			t.Errorf("Stop returned %v", err)
		}
	})

	within(t, 10*time.Second, "Cleanup with a blocked writer", func() {
		session.Cleanup()
	})

	within(t, 5*time.Second, "ReadFrame after Cleanup", func() {
		if _, err := session.ReadFrame(); err != io.EOF {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

// TestConcurrentStop fires many Stops at once. Exactly one may succeed, all the
// others must report ErrNotRunning, and nothing may panic or block.
func TestConcurrentStop(t *testing.T) {
	requireFFmpeg(t)

	options := *StdEncodeOptions
	options.BufferedFrames = 2

	session, err := EncodeFile("testaudio.ogg", &options)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	const callers = 8
	results := make(chan error, callers)
	within(t, 10*time.Second, "concurrent Stop", func() {
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results <- session.Stop()
			}()
		}
		wg.Wait()
	})
	close(results)

	succeeded, notRunning := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrNotRunning):
			notRunning++
		default:
			t.Errorf("unexpected Stop error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Errorf("%d Stop calls succeeded, expected exactly 1", succeeded)
	}
	if notRunning != callers-1 {
		t.Errorf("%d Stop calls reported ErrNotRunning, expected %d", notRunning, callers-1)
	}

	within(t, 10*time.Second, "Cleanup", func() {
		session.Cleanup()
	})
}

// TestEncodeWithFilters checks that a non-default Volume and an AudioFilter are
// accepted by whatever ffmpeg is installed. Volume used to be passed as the
// removed `-vol` option, so this pins down the replacement.
func TestEncodeWithFilters(t *testing.T) {
	requireFFmpeg(t)

	options := *StdEncodeOptions
	options.Volume = 128
	options.AudioFilter = "highpass=f=20"

	session, err := EncodeFile("testaudio.ogg", &options)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Cleanup()

	frames := 0
	for {
		if _, err := session.OpusFrame(); err != nil {
			if err != io.EOF {
				t.Errorf("read error: %v (ffmpeg said: %s)", err, session.FFMPEGMessages())
			}
			break
		}
		frames++
	}

	if frames < 700 {
		t.Errorf("only %d frames with filters, ffmpeg said: %s", frames, session.FFMPEGMessages())
	}
}

// TestReadAfterNaturalEnd checks that a finished session behaves the same as a
// stopped one from the caller's point of view.
func TestReadAfterNaturalEnd(t *testing.T) {
	requireFFmpeg(t)

	session, err := EncodeFile("testaudio.ogg", StdEncodeOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Cleanup()

	for {
		if _, err := session.ReadFrame(); err != nil {
			break
		}
	}

	within(t, 5*time.Second, "ReadFrame after end", func() {
		if _, err := session.ReadFrame(); err != io.EOF {
			t.Errorf("expected io.EOF, got %v", err)
		}
		if _, err := session.OpusFrame(); err != io.EOF {
			t.Errorf("OpusFrame expected io.EOF, got %v", err)
		}
		if _, err := session.Read(make([]byte, 16)); err != io.EOF {
			t.Errorf("Read expected io.EOF, got %v", err)
		}
	})
}
