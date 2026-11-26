package main

import (
	"errors"
	"image"
	"image/color"
	"image/gif"
	"os"
	"path/filepath"
	"testing"
)

func solidImage(w, h int, c color.RGBA) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for i := 0; i < len(img.Pix); i += 4 {
		img.Pix[i] = c.R
		img.Pix[i+1] = c.G
		img.Pix[i+2] = c.B
		img.Pix[i+3] = c.A
	}
	return img
}

func TestRecorderDelayMatchesFrameRate(t *testing.T) {
	if d := NewRecorder(60).Delay(); d != 1 {
		t.Errorf("60fps: expected delay 1, got %d", d)
	}
	if d := NewRecorder(30).Delay(); d != 3 {
		t.Errorf("30fps: expected delay 3, got %d", d)
	}
	if d := NewRecorder(120).Delay(); d != 1 {
		t.Errorf("120fps: expected clamped delay 1, got %d", d)
	}
	if d := NewRecorder(0).Delay(); d != 1 {
		t.Errorf("invalid fps falls back to 60: expected delay 1, got %d", d)
	}
}

func TestRecorderCaptureOnlyWhileRecording(t *testing.T) {
	rec := NewRecorder(60)
	img := solidImage(4, 4, color.RGBA{R: 255, A: 255})

	rec.Capture(img) // not recording yet
	if rec.FrameCount() != 0 {
		t.Fatalf("capture before Start must be ignored, got %d frames", rec.FrameCount())
	}

	rec.Start()
	rec.Capture(img)
	rec.Capture(img)
	if rec.FrameCount() != 2 {
		t.Fatalf("expected 2 frames, got %d", rec.FrameCount())
	}
}

func TestRecorderStartClearsPreviousRecording(t *testing.T) {
	rec := NewRecorder(60)
	img := solidImage(4, 4, color.RGBA{A: 255})

	rec.Start()
	rec.Capture(img)
	rec.Capture(img)

	// Restarting must discard the previous segment entirely.
	rec.Start()
	if rec.FrameCount() != 0 {
		t.Fatalf("Start must clear previous frames, got %d", rec.FrameCount())
	}
	if rec.Status() != "" || rec.LastError() != nil {
		t.Fatalf("Start must clear stale status/error, got %q / %v", rec.Status(), rec.LastError())
	}
}

func TestRecorderCaptureDeepCopiesFrames(t *testing.T) {
	rec := NewRecorder(60)
	img := solidImage(4, 4, color.RGBA{R: 10, A: 255})

	rec.Start()
	rec.Capture(img)

	// Mutate the source (simulates Space reset / R clear / palette switch
	// during recording): already-captured frames must not change.
	for i := range img.Pix {
		img.Pix[i] = 200
	}

	stored, ok := rec.frames[0].(*image.RGBA)
	if !ok {
		t.Fatalf("stored frame has type %T, want *image.RGBA", rec.frames[0])
	}
	if stored.Pix[0] != 10 {
		t.Errorf("captured frame was rewritten by later mutations: R = %d, want 10", stored.Pix[0])
	}
}

func TestRecorderStopWithoutFramesWritesNothing(t *testing.T) {
	rec := NewRecorder(60)
	rec.Start() // e.g. G pressed while paused: no frame ever advances

	path := filepath.Join(t.TempDir(), "empty.gif")
	err := rec.Stop(path)
	if !errors.Is(err, ErrNoFramesCaptured) {
		t.Fatalf("expected ErrNoFramesCaptured, got %v", err)
	}
	if rec.LastError() == nil || rec.Status() == "" {
		t.Error("empty stop must surface a HUD-visible status and error")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Error("no GIF file must be written for an empty recording")
	}
	if rec.Recording() {
		t.Error("recorder must not be recording after Stop")
	}
}

func TestRecorderStopWritesGIFWithFrameDelay(t *testing.T) {
	rec := NewRecorder(60)
	rec.Start()

	const n = 5
	for i := 0; i < n; i++ {
		rec.Capture(solidImage(8, 8, color.RGBA{R: uint8(i * 40), A: 255}))
	}

	path := filepath.Join(t.TempDir(), "anim.gif")
	if err := rec.Stop(path); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if rec.LastError() != nil {
		t.Fatalf("unexpected LastError: %v", rec.LastError())
	}
	if rec.Status() == "" {
		t.Error("successful save must set a HUD status with the path")
	}

	anim, err := decodeGIF(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(anim.Image) != n {
		t.Fatalf("GIF frame count = %d, want %d", len(anim.Image), n)
	}
	for i, d := range anim.Delay {
		if d != rec.Delay() {
			t.Errorf("frame %d delay = %d, want %d (canvas frame rate)", i, d, rec.Delay())
		}
	}

	// After a successful stop the recorder is clean for the next segment.
	if rec.FrameCount() != 0 || rec.Recording() {
		t.Error("recorder must be reset after Stop")
	}
}

func TestRecorderFailedSaveLeavesCleanState(t *testing.T) {
	rec := NewRecorder(60)
	rec.Start()
	rec.Capture(solidImage(4, 4, color.RGBA{A: 255}))

	// Force a write failure: a regular file used as a directory.
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	badPath := filepath.Join(blocker, "sub", "anim.gif")

	if err := rec.Stop(badPath); err == nil {
		t.Fatal("expected save to fail")
	}
	if rec.LastError() == nil || rec.Status() == "" {
		t.Error("failed save must surface a HUD-visible error status")
	}
	if rec.FrameCount() != 0 || rec.Recording() {
		t.Error("failed stop must not leave stale frames behind")
	}

	// Pressing G again starts a fresh, working recording.
	rec.Start()
	rec.Capture(solidImage(4, 4, color.RGBA{G: 255, A: 255}))
	goodPath := filepath.Join(t.TempDir(), "ok.gif")
	if err := rec.Stop(goodPath); err != nil {
		t.Fatalf("restart after failure must succeed, got %v", err)
	}
	anim, err := decodeGIF(t, goodPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(anim.Image) != 1 {
		t.Fatalf("restarted recording must contain exactly its own frames, got %d", len(anim.Image))
	}
}

// TestUpdateSimulationPauseGating drives the same per-frame helper the Draw
// loop uses and verifies recording tracks only frames where the sim advanced.
func TestUpdateSimulationPauseGating(t *testing.T) {
	sim := NewSimulation(60, 40)
	rec := NewRecorder(60)
	rec.Start()

	advanced := 0
	// Frames 0-2: running. Frames 3-5: paused. Frames 6-8: resumed.
	for frame := 0; frame < 9; frame++ {
		sim.paused = frame >= 3 && frame < 6
		if updateSimulation(sim, rec) {
			advanced++
		}
	}

	if advanced != 6 {
		t.Fatalf("expected 6 advanced frames, got %d", advanced)
	}
	if rec.FrameCount() != advanced {
		t.Fatalf("recorded frames = %d, want %d (paused frames must not record)", rec.FrameCount(), advanced)
	}

	// Reset mid-recording: subsequent frames reflect the new state, old ones intact.
	sim.Reset()
	if !updateSimulation(sim, rec) {
		t.Fatal("expected advance after reset")
	}
	if rec.FrameCount() != 7 {
		t.Fatalf("expected 7 frames after reset frame, got %d", rec.FrameCount())
	}

	path := filepath.Join(t.TempDir(), "sim.gif")
	if err := rec.Stop(path); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	anim, err := decodeGIF(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(anim.Image) != 7 {
		t.Fatalf("GIF frames = %d, want 7", len(anim.Image))
	}
}

// TestToggleRecordingPausedStart exercises the G-key edge case: starting a
// recording while paused, stopping before any real advance, then restarting.
func TestToggleRecordingPausedStart(t *testing.T) {
	sim := NewSimulation(60, 40)
	sim.paused = true
	rec := NewRecorder(60)
	path := filepath.Join(t.TempDir(), "toggle.gif")

	toggleRecording(rec, path) // G: start while paused
	if !rec.Recording() {
		t.Fatal("expected recording to start")
	}
	updateSimulation(sim, rec) // paused frame: nothing captured
	toggleRecording(rec, path) // G: stop with zero frames

	if !errors.Is(rec.LastError(), ErrNoFramesCaptured) {
		t.Fatalf("expected ErrNoFramesCaptured, got %v", rec.LastError())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("no file must be written when nothing advanced")
	}

	// Unpause, record one real frame, stop: now a non-empty GIF is produced.
	sim.paused = false
	toggleRecording(rec, path)
	updateSimulation(sim, rec)
	toggleRecording(rec, path)

	if rec.LastError() != nil {
		t.Fatalf("expected successful save, got %v", rec.LastError())
	}
	anim, err := decodeGIF(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(anim.Image) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(anim.Image))
	}
}

func decodeGIF(t *testing.T, path string) (*gif.GIF, error) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return gif.DecodeAll(f)
}
