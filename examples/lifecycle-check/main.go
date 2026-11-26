// Command lifecycle-check drives the transcoder through the two scenarios the
// task requires, using the real ffmpeg and ffprobe executables:
//
//  1. start a full transcode of the repository 3gp sample, let it run for a
//     moment, then Stop() midway and confirm the progress channel closes and
//     publishes nothing after the stop;
//  2. start the same transcoder twice, stopping the first run midway, and
//     confirm the second run's progress only reflects the second task.
//
// The first run is given an -itsoffset so its timeline is disjoint from the
// second run; a leaked line from the first task is therefore unmistakable.
//
// Run with:
//
//	go run ./examples/lifecycle-check -input ../../e2e/fixtures/input.3gp
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/xfrr/goffmpeg/pkg/duration"
	"github.com/xfrr/goffmpeg/transcoder"
)

const (
	waitForProgress = 30 * time.Second
	waitForClose    = 15 * time.Second

	firstRunOffsetSeconds = 20.0
	firstRunOffset        = "00:00:20.000"
)

func main() {
	input := flag.String("input", "e2e/fixtures/input.3gp", "path to the 3gp sample")
	outDir := flag.String("out", "", "directory for output files (default: a temp dir)")
	flag.Parse()

	if *outDir == "" {
		dir, err := os.MkdirTemp("", "goffmpeg-lifecycle-")
		check(err)
		*outDir = dir
	}
	check(os.MkdirAll(*outDir, 0o755))

	if _, err := os.Stat(*input); err != nil {
		fail("input sample not found at %s: %v", *input, err)
	}

	failed := false
	if !scenarioStopMidway(*input, *outDir) {
		failed = true
	}
	if !scenarioRestart(*input, *outDir) {
		failed = true
	}

	if failed {
		fmt.Println("\nRESULT: FAIL")
		os.Exit(1)
	}
	fmt.Println("\nRESULT: PASS")
}

// scenarioStopMidway runs a full encode, stops it midway and checks the
// progress channel closes without publishing anything after the stop.
func scenarioStopMidway(input, outDir string) bool {
	fmt.Println("== scenario 1: stop midway ==")

	output := filepath.Join(outDir, "stopped.mp4")
	trans := newTranscoder(input, output)
	slowDown(trans)

	done := trans.Run(true)
	progress := trans.Output()

	first, ok := recv(progress, waitForProgress)
	if !ok {
		fmt.Println("  FAIL: no progress before stop")
		return false
	}
	fmt.Printf("  progress before stop: frame=%s time=%s\n", first.FramesProcessed, first.CurrentTime)

	stopTime := time.Now()
	check(trans.Stop())
	stopTook := time.Since(stopTime)

	closed := false
	late := drain(progress, waitForClose, &closed)
	closeTook := time.Since(stopTime)

	err := <-done

	ok = true
	if !closed {
		fmt.Printf("  FAIL: progress channel still open %.1fs after stop\n", closeTook.Seconds())
		ok = false
	}
	if len(late) != 0 {
		fmt.Printf("  FAIL: %d progress value(s) published after stop: %+v\n", len(late), late[0])
		ok = false
	}
	if err != nil {
		fmt.Printf("  FAIL: stop reported an error: %v\n", err)
		ok = false
	}
	if ok {
		fmt.Printf("  OK: Stop returned in %.2fs, channel closed %.2fs after stop, no late progress\n",
			stopTook.Seconds(), closeTook.Seconds())
	}
	return ok
}

// scenarioRestart runs the same transcoder twice and checks that the second
// run's progress cannot contain lines from the stopped first task.
func scenarioRestart(input, outDir string) bool {
	fmt.Println("== scenario 2: start twice, progress isolated ==")

	trans := newTranscoder(input, filepath.Join(outDir, "first.mp4"))
	slowDown(trans)
	trans.MediaFile().SetInputInitialOffset(firstRunOffset)

	done1 := trans.Run(true)
	progress1 := trans.Output()

	first, ok := recv(progress1, waitForProgress)
	if !ok {
		fmt.Println("  FAIL: first run produced no progress")
		return false
	}
	fmt.Printf("  first run progress: frame=%s time=%s\n", first.FramesProcessed, first.CurrentTime)
	if duration.DurToSec(first.CurrentTime) < firstRunOffsetSeconds {
		fmt.Printf("  FAIL: first run did not honor the offset, got time=%s\n", first.CurrentTime)
		return false
	}

	check(trans.Stop())
	var closed bool
	drain(progress1, waitForClose, &closed)
	if !closed {
		fmt.Println("  FAIL: first run's channel did not close after stop")
		return false
	}
	if err := <-done1; err != nil {
		fmt.Printf("  FAIL: first run finished with an error: %v\n", err)
		return false
	}
	fmt.Println("  first run stopped, channel closed")

	// Restart the same transcoder with no offset and a fresh output.
	check(trans.SetOutputPath(filepath.Join(outDir, "second.mp4")))
	trans.MediaFile().SetInputInitialOffset("")
	slowDown(trans)

	done2 := trans.Run(true)
	progress2 := trans.Output()

	seen := 0
	deadline := time.After(waitForProgress)
	for {
		select {
		case p, ok := <-progress2:
			if !ok {
				if err := <-done2; err != nil {
					fmt.Printf("  FAIL: second run ended with an error: %v\n", err)
					return false
				}
				fmt.Printf("  second run finished on its own after %d progress value(s)\n", seen)
				if seen == 0 {
					return false
				}
				return true
			}
			seen++
			secs := duration.DurToSec(p.CurrentTime)
			fmt.Printf("  second run progress: frame=%s time=%s\n", p.FramesProcessed, p.CurrentTime)
			if secs >= firstRunOffsetSeconds {
				fmt.Printf("  FAIL: second run reported a first-task timestamp (%.1fs >= offset %.1fs)\n",
					secs, firstRunOffsetSeconds)
				check(trans.Stop())
				<-done2
				return false
			}
			if seen >= 3 {
				check(trans.Stop())
				var c bool
				rest := drain(progress2, waitForClose, &c)
				if len(rest) != 0 {
					fmt.Printf("  FAIL: second run published after stop: %+v\n", rest[0])
					return false
				}
				<-done2
				fmt.Println("  OK: second run's progress stayed within the second task's timeline")
				return true
			}
		case <-deadline:
			fmt.Println("  FAIL: second run produced no progress")
			return false
		}
	}
}

// encodeFinished reports whether a statistics line already covers the whole
// input, meaning the encode could not have been interrupted mid-flight.
func encodeFinished(trans *transcoder.Transcoder, p transcoder.Progress) bool {
	total, _ := strconv.ParseFloat(trans.MediaFile().Metadata().Format.Duration, 64)
	if total == 0 {
		return false
	}
	return duration.DurToSec(p.CurrentTime) >= total-0.5
}

func newTranscoder(input, output string) *transcoder.Transcoder {
	trans := new(transcoder.Transcoder)
	check(trans.Initialize(input, output))
	return trans
}

// slowDown makes a full encode take long enough to be interrupted mid-flight:
// read at real time and upscale, so the encoder cannot finish in one burst.
func slowDown(trans *transcoder.Transcoder) {
	trans.MediaFile().SetNativeFramerateInput(true)
	trans.MediaFile().SetVideoCodec("libx264")
	trans.MediaFile().SetPreset("veryslow")
	trans.MediaFile().SetResolution("640x480")
}

func recv(ch <-chan transcoder.Progress, timeout time.Duration) (transcoder.Progress, bool) {
	select {
	case p, ok := <-ch:
		return p, ok
	case <-time.After(timeout):
		return transcoder.Progress{}, false
	}
}

func drain(ch <-chan transcoder.Progress, timeout time.Duration, closed *bool) []transcoder.Progress {
	var values []transcoder.Progress
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

func check(err error) {
	if err != nil {
		fail("unexpected error: %v", err)
	}
}

func fail(format string, args ...interface{}) {
	fmt.Printf("ERROR: "+format+"\n", args...)
	os.Exit(2)
}
