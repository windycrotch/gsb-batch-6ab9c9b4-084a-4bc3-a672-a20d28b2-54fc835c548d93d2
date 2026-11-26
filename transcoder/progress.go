package transcoder

import (
	"bytes"
	"regexp"
	"strings"

	"github.com/xfrr/goffmpeg/pkg/duration"
)

type Progress struct {
	FramesProcessed string
	CurrentTime     string
	CurrentBitrate  string
	Progress        float64
	Speed           string
}

var progressSeparator = regexp.MustCompile(`=\s+`)

// scanProgressLines splits ffmpeg stderr into progress-sized tokens.
//
// ffmpeg refreshes its statistics line with a carriage return and, on some
// platforms, still terminates it with \n. Both terminators are handled so a
// single logical progress line is never split or merged with the next one.
func scanProgressLines(data []byte, atEOF bool) (advance int, token []byte, splitErr error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	// windows \r\n
	// so first \r and then \n can remove unexpected line break
	if i := bytes.IndexByte(data, '\r'); i >= 0 {
		// We have a cr terminated line
		return i + 1, data[0:i], nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0:i], nil
	}
	if atEOF {
		return len(data), data, nil
	}

	return 0, nil, nil
}

// parseProgressLine converts a statistics line into a Progress value.
//
// durationSec is the input duration captured by the run that produced the
// line, never a value read from the transcoder's current media file, so a
// restarted transcoder cannot reinterpret stale output in terms of the new
// task.
func parseProgressLine(line string, durationSec float64) (Progress, bool) {
	if !strings.Contains(line, "frame=") ||
		!strings.Contains(line, "time=") ||
		!strings.Contains(line, "bitrate=") {
		return Progress{}, false
	}

	progress := Progress{}

	st := progressSeparator.ReplaceAllString(line, `=`)
	fields := strings.Fields(st)

	var framesProcessed, currentTime, currentBitrate, currentSpeed string
	for _, field := range fields {
		fieldSplit := strings.Split(field, "=")
		if len(fieldSplit) <= 1 {
			continue
		}

		switch fieldSplit[0] {
		case "frame":
			framesProcessed = fieldSplit[1]
		case "time":
			currentTime = fieldSplit[1]
		case "bitrate":
			currentBitrate = fieldSplit[1]
		case "speed":
			currentSpeed = fieldSplit[1]
		}
	}

	timesec := duration.DurToSec(currentTime)
	// live stream check
	if durationSec != 0 {
		progress.Progress = (timesec * 100) / durationSec
	}
	progress.CurrentBitrate = currentBitrate
	progress.FramesProcessed = framesProcessed
	progress.CurrentTime = currentTime
	progress.Speed = currentSpeed

	return progress, true
}
