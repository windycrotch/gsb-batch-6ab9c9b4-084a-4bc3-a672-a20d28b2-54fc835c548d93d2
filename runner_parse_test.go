package spireav

import (
	"context"
	"io"
	"testing"
)

// chunkedReader returns the given input in fixed-size chunks, optionally with an error
// injected at a specific offset. It exercises the parser's handling of output that
// arrives in arbitrary fragments.
type chunkedReader struct {
	data      string
	chunkSize int
	pos       int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	end := r.pos + r.chunkSize
	if end > len(r.data) {
		end = len(r.data)
	}
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	return n, nil
}

func collectProgress(t *testing.T, input string, chunkSize int) []Progress {
	t.Helper()
	var reports []Progress
	runner := &implRunner{}
	delivery := newProgressDelivery(context.Background(), func(p Progress) {
		reports = append(reports, p)
	})
	runner.reportFFmpegProgress(&chunkedReader{data: input, chunkSize: chunkSize}, delivery)
	return reports
}

func TestParseProgressChunking(t *testing.T) {
	blocks := "frame=1\nfps=25.0\nout_time=00:00:01.000000\nprogress=continue\n" +
		"frame=2\nfps=25.0\nout_time=00:00:02.000000\nprogress=end\n"

	// Feed the input one byte at a time to prove that a fragmented line never breaks
	// or duplicates a callback.
	reports := collectProgress(t, blocks, 1)
	if len(reports) != 2 {
		t.Fatalf("expected 2 progress reports, got %d: %+v", len(reports), reports)
	}
	if reports[0].Frame != 1 || reports[0].Done {
		t.Fatalf("unexpected first report: %+v", reports[0])
	}
	if reports[1].Frame != 2 || !reports[1].Done {
		t.Fatalf("unexpected second report: %+v", reports[1])
	}

	// The same input in one chunk must produce the same result.
	whole := collectProgress(t, blocks, len(blocks))
	if len(whole) != 2 || whole[0].Frame != 1 || whole[1].Frame != 2 || !whole[1].Done {
		t.Fatalf("chunking changed the result: %+v", whole)
	}
}

func TestParseProgressUnrecognizedLines(t *testing.T) {
	input := "garbage without separator\n" +
		"\n" +
		"=empty key\n" +
		"unknown_key=some value\n" +
		"frame=7\n" +
		"bitrate=N/A\n" +
		"progress=continue\n"
	reports := collectProgress(t, input, 3)
	if len(reports) != 1 {
		t.Fatalf("expected 1 progress report, got %d: %+v", len(reports), reports)
	}
	if reports[0].Frame != 7 {
		t.Fatalf("expected frame 7, got %d", reports[0].Frame)
	}
	if reports[0].Done {
		t.Fatalf("expected progress=continue, got done")
	}
}

func TestParseProgressDuplicateKeys(t *testing.T) {
	// Two progress=continue keys for the same block, with no new fields in between, must
	// be delivered only once. A following block with fresh fields is delivered again.
	input := "frame=1\nprogress=continue\nprogress=continue\n" +
		"frame=2\nprogress=continue\nprogress=continue\n" +
		"progress=end\n"
	reports := collectProgress(t, input, 4)
	if len(reports) != 3 {
		t.Fatalf("expected 3 progress reports, got %d: %+v", len(reports), reports)
	}
	if reports[0].Frame != 1 || reports[1].Frame != 2 || !reports[2].Done {
		t.Fatalf("unexpected reports: %+v", reports)
	}
}

func TestParseProgressTrailingFragment(t *testing.T) {
	// The stream ends with a key/value line that has no trailing newline. It must still
	// be parsed and delivered.
	input := "frame=1\nprogress=end"
	reports := collectProgress(t, input, 5)
	if len(reports) != 1 {
		t.Fatalf("expected 1 progress report, got %d: %+v", len(reports), reports)
	}
	if reports[0].Frame != 1 || !reports[0].Done {
		t.Fatalf("unexpected report: %+v", reports[0])
	}
}

func TestDeliveryElapsedMonotonic(t *testing.T) {
	var reports []Progress
	delivery := newProgressDelivery(context.Background(), func(p Progress) {
		reports = append(reports, p)
	})
	for i := 0; i < 5; i++ {
		delivery.deliver(Progress{Frame: int64(i)})
	}
	if len(reports) != 5 {
		t.Fatalf("expected 5 reports, got %d", len(reports))
	}
	for i := 1; i < len(reports); i++ {
		if reports[i].Elapsed < reports[i-1].Elapsed {
			t.Fatalf("elapsed decreased at %d: %v < %v", i, reports[i].Elapsed, reports[i-1].Elapsed)
		}
	}
}

func TestDeliveryClosedStopsCallbacks(t *testing.T) {
	var calls int
	delivery := newProgressDelivery(context.Background(), func(Progress) { calls++ })
	delivery.deliver(Progress{})
	delivery.close()
	delivery.deliver(Progress{})
	delivery.finishSuccess()
	if calls != 1 {
		t.Fatalf("expected callbacks to stop after close, got %d", calls)
	}
}

func TestDeliveryRespectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	delivery := newProgressDelivery(ctx, func(Progress) { calls++ })
	delivery.deliver(Progress{})
	cancel()
	delivery.deliver(Progress{})
	if calls != 1 {
		t.Fatalf("expected no callback after cancellation, got %d", calls)
	}
}

func TestDeliveryFinishSuccess(t *testing.T) {
	var reports []Progress
	delivery := newProgressDelivery(context.Background(), func(p Progress) {
		reports = append(reports, p)
	})
	delivery.finishSuccess() // no prior progress: still emits a done report
	if len(reports) != 1 || !reports[0].Done {
		t.Fatalf("expected one done report, got %+v", reports)
	}
	delivery.deliver(Progress{Frame: 3})
	delivery.finishSuccess() // already done: must not emit another report
	if len(reports) != 1 {
		t.Fatalf("expected no additional report after done, got %d", len(reports))
	}
}

func TestParseProgressNoCallbackDoesNotPanic(t *testing.T) {
	// When no callback is configured, delivery must still be safe to call.
	delivery := newProgressDelivery(context.Background(), nil)
	delivery.deliver(Progress{})
	delivery.finishSuccess()
}
