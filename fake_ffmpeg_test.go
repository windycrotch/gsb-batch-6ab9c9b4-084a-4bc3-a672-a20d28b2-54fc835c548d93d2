package spireav_test

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// The tests in this package must not depend on a real FFmpeg binary. Instead, the test
// binary re-executes itself and acts as a stand-in for FFmpeg: TestMain detects the
// environment variable below and runs the fake implementation instead of the tests.
const fakeFfmpegEnv = "SPIREAV_TEST_FAKE_FFMPEG"

func TestMain(m *testing.M) {
	if os.Getenv(fakeFfmpegEnv) == "1" {
		os.Exit(fakeFfmpegMain(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// fakeFfmpegMain emulates the subset of FFmpeg behavior the runner cares about.
// The first argument selects the mode; additional arguments are mode-specific.
func fakeFfmpegMain(args []string) int {
	mode := "success"
	if len(args) > 0 {
		mode = args[0]
	}

	switch mode {
	case "success":
		// A normal run that reports its own end.
		writeProgressBlock(os.Stdout, 0, "continue")
		writeProgressBlock(os.Stdout, 1, "continue")
		writeProgressBlock(os.Stdout, 2, "end")
		return 0

	case "success-no-end":
		// A normal run whose output never contains a "progress=end" line.
		writeProgressBlock(os.Stdout, 0, "continue")
		writeProgressBlock(os.Stdout, 1, "continue")
		return 0

	case "failure":
		// Emits a progress block, then fails with logs on stderr.
		writeProgressBlock(os.Stdout, 0, "continue")
		fmt.Fprintln(os.Stderr, "Invalid argument: no such input file")
		fmt.Fprintln(os.Stderr, "Conversion failed!")
		return 3

	case "malformed":
		// Splits lines into arbitrary fragments, interleaves unrecognized lines, and
		// repeats the progress key of a block.
		fragments := []string{
			"fra", "me=1\nfps=", "25.0\nnoise without a separator\n",
			"bitrate=1000.0kbits/s\ntotal_size=2048\nprogress=continue\n",
			"progress=continue\n", // duplicate progress key for the same block
			"out_time=00:00:01.000000\n",
			"unknown_key=whatever\n", "drop_frames=0\nspeed=1.0x\n",
			"progress=continue\n",
			"frame=2\nfps=25.0\nbitrate=1000.0kbits/s\ntotal_size=4096\n",
			"out_time=00:00:02.000000\ndup_frames=0\ndrop_frames=0\nspeed=1.0x\n",
			"progress=end", // final line has no trailing newline
		}
		for _, fragment := range fragments {
			if _, err := os.Stdout.WriteString(fragment); err != nil {
				return 1
			}
			time.Sleep(2 * time.Millisecond)
		}
		return 0

	case "cancel":
		// Reports a couple of blocks, waits for the test to allow it to continue, then
		// floods the output. The test cancels the context before allowing the flood.
		readyPath, goPath := args[1], args[2]
		writeProgressBlock(os.Stdout, 0, "continue")
		writeProgressBlock(os.Stdout, 1, "continue")
		if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
			return 1
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(goPath); err == nil {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		for i := 0; i < 200; i++ {
			writeProgressBlock(os.Stdout, int64(i+2), "continue")
			time.Sleep(time.Millisecond)
		}
		return 0

	case "hang":
		// Never terminates on its own; used for process-level cancellation checks.
		writeProgressBlock(os.Stdout, 0, "continue")
		time.Sleep(30 * time.Second)
		return 0

	default:
		fmt.Fprintf(os.Stderr, "unknown fake ffmpeg mode %q\n", mode)
		return 2
	}
}

func writeProgressBlock(w *os.File, frame int64, progress string) {
	fmt.Fprintf(w,
		"frame=%d\nfps=25.0\nbitrate=1000.0kbits/s\ntotal_size=%d\n"+
			"out_time=00:00:01.000000\ndup_frames=0\ndrop_frames=0\nspeed=1.0x\nprogress=%s\n",
		frame, (frame+1)*1024, progress)
}
