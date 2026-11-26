package main

import (
	"errors"
	"fmt"
	"image"

	"github.com/h8gi/canvas"
)

// ErrNoFramesCaptured is returned when a recording is stopped before the
// simulation advanced at least one frame (e.g. recording started while paused).
var ErrNoFramesCaptured = errors.New("no frames captured: simulation never advanced")

// Recorder captures rendered simulation frames and encodes them into an
// animated GIF via the canvas library's public SaveGIF / GIFOptions API.
// It does not use the canvas package's internal recorder; frames are only
// appended when Capture is called, so pausing the simulation pauses the
// recording as well.
type Recorder struct {
	frameRate int
	recording bool
	frames    []image.Image
	status    string
	lastErr   error
}

// NewRecorder creates a Recorder whose GIF frame delay matches frameRate FPS.
func NewRecorder(frameRate int) *Recorder {
	if frameRate <= 0 {
		frameRate = 60
	}
	return &Recorder{frameRate: frameRate}
}

// Delay returns the GIF frame delay in 1/100s units matching the canvas frame rate.
func (r *Recorder) Delay() int {
	d := 100 / r.frameRate
	if d < 1 {
		d = 1
	}
	return d
}

// Start begins a new recording, discarding any previously captured frames and
// clearing the last status so no stale state carries over.
func (r *Recorder) Start() {
	r.frames = nil
	r.recording = true
	r.status = ""
	r.lastErr = nil
}

// Recording reports whether a recording is currently in progress.
func (r *Recorder) Recording() bool {
	return r.recording
}

// FrameCount returns the number of frames captured so far.
func (r *Recorder) FrameCount() int {
	return len(r.frames)
}

// Status returns the message describing the outcome of the last stop attempt.
func (r *Recorder) Status() string {
	return r.status
}

// LastError returns the error produced by the last failed stop, if any.
func (r *Recorder) LastError() error {
	return r.lastErr
}

// Capture stores a deep copy of img as one animation frame. It is a no-op
// when not recording. The copy guarantees that later resets, clears, or
// preset/palette changes never rewrite already-recorded frames.
func (r *Recorder) Capture(img *image.RGBA) {
	if !r.recording || img == nil {
		return
	}
	frame := image.NewRGBA(img.Bounds())
	copy(frame.Pix, img.Pix)
	r.frames = append(r.frames, frame)
}

// Stop ends the recording and writes the captured frames to path as an
// animated GIF whose frame delay matches the canvas frame rate. If no frame
// was captured it returns ErrNoFramesCaptured and writes nothing. Captured
// frames are always discarded, so a failed stop leaves no stale state behind
// and a fresh Start begins cleanly.
func (r *Recorder) Stop(path string) error {
	r.recording = false
	frames := r.frames
	r.frames = nil

	if len(frames) == 0 {
		r.lastErr = ErrNoFramesCaptured
		r.status = "Nothing recorded: simulation never advanced (was it paused?)"
		return r.lastErr
	}

	err := canvas.SaveGIF(path, frames, &canvas.GIFOptions{Delay: r.Delay()})
	if err != nil {
		r.lastErr = err
		r.status = fmt.Sprintf("GIF save failed: %v", err)
		return err
	}

	r.lastErr = nil
	r.status = fmt.Sprintf("Saved %d frames to %s", len(frames), path)
	return nil
}
