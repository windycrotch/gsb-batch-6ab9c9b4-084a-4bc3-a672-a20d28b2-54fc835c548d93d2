package astisub_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asticode/go-astisub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Local, self-made samples and helpers
// ---------------------------------------------------------------------------

// writeSampleSRT writes a tiny valid SRT carrying a unique marker.
func writeSampleSRT(t *testing.T, dir, name string, n int) string {
	t.Helper()
	content := fmt.Sprintf("1\n00:00:%02d,000 --> 00:00:%02d,000\nMarker-%02d hello\n\n", n%50, (n+1)%50, n)
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	return p
}

// writeBrokenSRT writes an SRT whose end timestamp cannot be parsed, so the
// existing ReadFromSRT chain returns an error.
func writeBrokenSRT(t *testing.T, dir, name string) string {
	t.Helper()
	content := "1\n00:00:01,000 --> not-a-timestamp\nThis cue has a broken boundary\n\n"
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	return p
}

// richTTMLWithStyles is a self-made TTML sample referencing an item style
// (with a parent), an inline-span style and a region (with its own style),
// while also declaring unreferenced styles and a region.
const richTTMLWithStyles = `<?xml version="1.0" encoding="UTF-8"?>
<tt xmlns="http://www.w3.org/ns/ttml" xmlns:tts="http://www.w3.org/ns/ttml#styling" xml:lang="en">
  <head>
    <styling>
      <style xml:id="parent-style" tts:fontFamily="Serif"/>
      <style xml:id="item-style" style="parent-style" tts:color="red"/>
      <style xml:id="span-style" tts:fontStyle="italic"/>
      <style xml:id="region-style" tts:textAlign="center"/>
      <style xml:id="unused-style" tts:color="blue"/>
    </styling>
    <layout>
      <region xml:id="used-region" style="region-style" tts:origin="10% 80%"/>
      <region xml:id="unused-region" tts:origin="20% 20%"/>
    </layout>
  </head>
  <body>
    <div>
      <p begin="00:00:01.000" end="00:00:02.000" style="item-style" region="used-region">Hello <span style="span-style">styled</span></p>
    </div>
  </body>
</tt>
`

func writeRichTTML(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(richTTMLWithStyles), 0o644))
	return p
}

func writeBytes(t *testing.T, p string, b []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, b, 0o644))
}

func readBytes(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	return b
}

func assertBytesEqual(t *testing.T, want, got []byte, msgAndArgs ...interface{}) {
	t.Helper()
	assert.Equal(t, want, got, msgAndArgs...)
}

// referenceSingleWrite produces the expected bytes using the stock
// Open + Subtitles.Write chain.
func referenceSingleWrite(t *testing.T, src, refPath string) []byte {
	t.Helper()
	s, err := astisub.Open(astisub.Options{Filename: src})
	require.NoError(t, err)
	require.NoError(t, s.Write(refPath))
	return readBytes(t, refPath)
}

func assertNoTempResidue(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".**.astisub-tmp-*"))
	require.NoError(t, err)
	assert.Empty(t, matches, "temporary files must be removed: %v", matches)
}

// resultProjection is the comparable, stable view of a result used for
// idempotency assertions.
type resultProjection struct {
	Index     int
	Source    string
	Target    string
	OK        bool
	Cancelled bool
	Reason    string
}

func projectResults(rs []astisub.BatchResult) []resultProjection {
	out := make([]resultProjection, len(rs))
	for i, r := range rs {
		out[i] = resultProjection{
			Index:     r.Index,
			Source:    r.Source,
			Target:    r.Target,
			OK:        r.OK,
			Cancelled: r.Cancelled,
			Reason:    r.Reason,
		}
	}
	return out
}

func assertOrderedIndices(t *testing.T, rs []astisub.BatchResult) {
	t.Helper()
	for i, r := range rs {
		assert.Equalf(t, i, r.Index, "result at position %d has wrong Index", i)
	}
}

// ---------------------------------------------------------------------------
// A1: all-success batch -> ordered one-to-one results, content parity with a
// single-file conversion.
// ---------------------------------------------------------------------------

func TestConvertBatch_AllSuccess_OrderAndParity(t *testing.T) {
	dir := t.TempDir()
	refDir := t.TempDir()

	type pair struct{ src, target, ref, marker string }
	var pairs []pair
	var jobs []astisub.BatchJob
	for i, ext := range []string{".ttml", ".vtt", ".ttml"} {
		src := writeSampleSRT(t, dir, fmt.Sprintf("in-%d.srt", i), i+1)
		target := filepath.Join(dir, fmt.Sprintf("out-%d%s", i, ext))
		ref := filepath.Join(refDir, fmt.Sprintf("ref-%d%s", i, ext))
		marker := fmt.Sprintf("Marker-%02d", i+1)
		pairs = append(pairs, pair{src: src, target: target, ref: ref, marker: marker})
		jobs = append(jobs, astisub.BatchJob{Source: src, Target: target})
	}

	results := astisub.ConvertBatch(context.Background(), jobs, astisub.WithConcurrency(3))

	require.Len(t, results, 3)
	assertOrderedIndices(t, results)
	seen := map[string]bool{}
	for i, r := range results {
		assert.Equal(t, jobs[i].Source, r.Source)
		assert.Equal(t, jobs[i].Target, r.Target)
		assert.True(t, r.OK, "job %d should succeed: %v", i, r.Err)
		assert.NoError(t, r.Err)
		assert.Empty(t, r.Reason)
		assert.False(t, r.Cancelled)
		assert.False(t, seen[r.Source], "input appeared twice")
		seen[r.Source] = true

		// Output exists, carries its own content and is byte-identical to the
		// stock single-file Open + Write conversion.
		got := readBytes(t, r.Target)
		assert.Contains(t, string(got), pairs[i].marker)
		want := referenceSingleWrite(t, pairs[i].src, pairs[i].ref)
		assertBytesEqual(t, want, got)
	}
	assert.Len(t, seen, 3)
	assertNoTempResidue(t, dir)
}

// ---------------------------------------------------------------------------
// A2: one corrupt SRT fails alone with a reason; other files still succeed.
// A pre-existing target of the failed parse must be untouched.
// ---------------------------------------------------------------------------

func TestConvertBatch_ParseFailureIsolated(t *testing.T) {
	dir := t.TempDir()
	srcGood1 := writeSampleSRT(t, dir, "good1.srt", 10)
	srcBad := writeBrokenSRT(t, dir, "broken.srt")
	srcGood2 := writeSampleSRT(t, dir, "good2.srt", 11)

	badTarget := filepath.Join(dir, "broken.ttml")
	sentinel := []byte("KEEP-ME-PARSE-FAILURE\n")
	writeBytes(t, badTarget, sentinel)

	jobs := []astisub.BatchJob{
		{Source: srcGood1, Target: filepath.Join(dir, "good1.ttml")},
		{Source: srcBad, Target: badTarget},
		{Source: srcGood2, Target: filepath.Join(dir, "good2.vtt")},
	}
	results := astisub.ConvertBatch(context.Background(), jobs, astisub.WithConcurrency(3))

	require.Len(t, results, 3)
	assertOrderedIndices(t, results)

	assert.True(t, results[0].OK)
	assert.False(t, results[1].OK)
	assert.Error(t, results[1].Err)
	assert.NotEmpty(t, results[1].Reason)
	assert.Contains(t, results[1].Reason, "parsing")
	assert.True(t, results[2].OK, "other files must still be written: %v", results[2].Err)

	// Failed parse never truncates or replaces an existing target.
	assertBytesEqual(t, sentinel, readBytes(t, badTarget))

	// Other outputs are complete and correct.
	assert.Contains(t, string(readBytes(t, jobs[0].Target)), "Marker-10")
	assert.Contains(t, string(readBytes(t, jobs[2].Target)), "Marker-11")
	assertNoTempResidue(t, dir)
}

// ---------------------------------------------------------------------------
// A3: injected mid-write failure preserves pre-existing target bytes. Also
// covers a real invalid-extension dispatch failure and a real rename failure.
// ---------------------------------------------------------------------------

func TestConvertBatch_WriteFailure_PreservesExistingTarget(t *testing.T) {
	t.Run("injected renderer failure", func(t *testing.T) {
		dir := t.TempDir()
		src := writeSampleSRT(t, dir, "in.srt", 1)
		target := filepath.Join(dir, "out.ttml")
		sentinel := []byte("ORIGINAL-TARGET-BYTES-v1\ndo not overwrite\n")
		writeBytes(t, target, sentinel)

		injected := errors.New("injected mid-write disk failure")
		conv := astisub.NewBatchConverter(
			astisub.WithRenderer(func(ctx context.Context, s *astisub.Subtitles, w io.Writer, target string) error {
				_, _ = w.Write([]byte("partial half-written content"))
				return injected
			}),
		)
		results := conv.Convert(context.Background(), []astisub.BatchJob{{Source: src, Target: target}})

		require.Len(t, results, 1)
		assert.False(t, results[0].OK)
		assert.True(t, errors.Is(results[0].Err, injected), "wrapped error must expose cause: %v", results[0].Err)
		assert.Contains(t, results[0].Reason, "injected mid-write disk failure")
		assert.False(t, results[0].Cancelled)
		assertBytesEqual(t, sentinel, readBytes(t, target))
		assertNoTempResidue(t, dir)
	})

	t.Run("invalid extension through stock chain", func(t *testing.T) {
		dir := t.TempDir()
		src := writeSampleSRT(t, dir, "in.srt", 1)
		target := filepath.Join(dir, "out.unsupported-ext")
		sentinel := []byte("binary-ish original contents \x00\x01\x02\n")
		writeBytes(t, target, sentinel)

		results := astisub.ConvertBatch(context.Background(), []astisub.BatchJob{{Source: src, Target: target}})
		require.Len(t, results, 1)
		require.False(t, results[0].OK)
		assert.True(t, errors.Is(results[0].Err, astisub.ErrInvalidExtension))
		assertBytesEqual(t, sentinel, readBytes(t, target))
		assertNoTempResidue(t, dir)
	})

	t.Run("rename onto existing directory fails safely", func(t *testing.T) {
		dir := t.TempDir()
		src := writeSampleSRT(t, dir, "in.srt", 1)
		target := filepath.Join(dir, "adirectory.ttml")
		require.NoError(t, os.Mkdir(target, 0o755))

		results := astisub.ConvertBatch(context.Background(), []astisub.BatchJob{{Source: src, Target: target}})
		require.Len(t, results, 1)
		assert.False(t, results[0].OK)
		info, err := os.Stat(target)
		require.NoError(t, err)
		assert.True(t, info.IsDir(), "target directory must still exist")
		assertNoTempResidue(t, dir)
	})
}

// WriteFileAtomic direct primitive: creates new files and overwrites existing
// ones with complete content.
func TestWriteFileAtomic_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := astisub.ReadFromSRT(strings.NewReader("1\n00:00:01,000 --> 00:00:02,000\nAtomic hello\n\n"))
	require.NoError(t, err)

	target := filepath.Join(dir, "a.ttml")
	require.NoError(t, astisub.WriteFileAtomic(target, s))
	first := readBytes(t, target)
	assert.Contains(t, string(first), "Atomic hello")

	// Replace with VTT content at a different path then overwrite same path via
	// TTML again: always complete.
	vtt := filepath.Join(dir, "a.vtt")
	require.NoError(t, astisub.WriteFileAtomic(vtt, s))
	assert.Contains(t, string(readBytes(t, vtt)), "WEBVTT")

	require.NoError(t, astisub.WriteFileAtomic(target, s))
	assertBytesEqual(t, first, readBytes(t, target))
	assertNoTempResidue(t, dir)
}

// failAfterNWriter fails a Write once more than failAfter bytes have gone
// through successfully.
type failAfterNWriter struct {
	w         io.Writer
	written   int
	failAfter int
	err       error
}

func (f *failAfterNWriter) Write(p []byte) (int, error) {
	if f.written >= f.failAfter {
		return 0, f.err
	}
	n := len(p)
	if f.written+n > f.failAfter {
		partial := p[:f.failAfter-f.written]
		m, _ := f.w.Write(partial)
		f.written += m
		return m, f.err
	}
	n, err := f.w.Write(p)
	f.written += n
	return n, err
}

// A failing writer injected through WithRenderer + WriteToWriter must abort
// the commit and preserve an existing target even when the real TTML chain is
// used.
func TestConvertBatch_InjectedFailingWriterThroughStockChain(t *testing.T) {
	dir := t.TempDir()
	src := writeSampleSRT(t, dir, "in.srt", 1)
	target := filepath.Join(dir, "out.ttml")
	sentinel := []byte("SENTINEL-FOR-WRITER-FAILURE\n")
	writeBytes(t, target, sentinel)

	writerErr := errors.New("injected ENOSPC-ish writer failure")
	conv := astisub.NewBatchConverter(
		astisub.WithRenderer(func(ctx context.Context, s *astisub.Subtitles, w io.Writer, target string) error {
			fw := &failAfterNWriter{w: w, failAfter: 32, err: writerErr}
			return astisub.WriteToWriter(s, fw, target)
		}),
	)
	rs := conv.Convert(context.Background(), []astisub.BatchJob{{Source: src, Target: target}})
	require.Len(t, rs, 1)
	assert.False(t, rs[0].OK)
	assert.True(t, errors.Is(rs[0].Err, writerErr), "%v", rs[0].Err)
	assertBytesEqual(t, sentinel, readBytes(t, target))
	assertNoTempResidue(t, dir)
}

// AtomicWrite preserves existing bytes when the write callback fails and
// replaces them when it succeeds.
func TestAtomicWrite_PreservesAndReplaces(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "file.txt")
	sentinel := []byte("before")
	writeBytes(t, target, sentinel)

	boom := errors.New("boom from write callback")
	err := astisub.AtomicWrite(target, func(w io.Writer) error {
		_, _ = w.Write([]byte("partial"))
		return boom
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, boom))
	assertBytesEqual(t, sentinel, readBytes(t, target))

	require.NoError(t, astisub.AtomicWrite(target, func(w io.Writer) error {
		_, err := w.Write([]byte("complete content"))
		return err
	}))
	assertBytesEqual(t, []byte("complete content"), readBytes(t, target))
	assertNoTempResidue(t, dir)

	// New file creation through the same primitive.
	fresh := filepath.Join(dir, "sub", "fresh.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(fresh), 0o755))
	require.NoError(t, astisub.AtomicWrite(fresh, func(w io.Writer) error {
		_, err := w.Write([]byte("fresh"))
		return err
	}))
	assertBytesEqual(t, []byte("fresh"), readBytes(t, fresh))
}

// ---------------------------------------------------------------------------
// A4: concurrent batch keeps input order, no duplicates / omissions. Also
// proves jobs actually run in parallel when requested.
// ---------------------------------------------------------------------------

func TestConvertBatch_ConcurrentOrderUniquenessAndContent(t *testing.T) {
	const n = 40
	dir := t.TempDir()
	jobs := make([]astisub.BatchJob, n)
	for i := 0; i < n; i++ {
		src := writeSampleSRT(t, dir, fmt.Sprintf("in-%02d.srt", i), i)
		jobs[i] = astisub.BatchJob{Source: src, Target: filepath.Join(dir, fmt.Sprintf("out-%02d.ttml", i))}
	}

	for run := 0; run < 3; run++ {
		results := astisub.ConvertBatch(context.Background(), jobs, astisub.WithConcurrency(8))
		require.Len(t, results, n)
		assertOrderedIndices(t, results)

		seen := map[string]int{}
		for i, r := range results {
			assert.True(t, r.OK, "run %d job %d failed: %v", run, i, r.Err)
			assert.Equal(t, i, r.Index)
			seen[r.Source]++
			got := readBytes(t, r.Target)
			assert.Contains(t, string(got), fmt.Sprintf("Marker-%02d", i))
		}
		assert.Len(t, seen, n, "every input must appear exactly once")
		for src, count := range seen {
			assert.Equalf(t, 1, count, "input %s appeared %d times", src, count)
		}
	}
	assertNoTempResidue(t, dir)
}

func TestConvertBatch_ActuallyRunsJobsConcurrently(t *testing.T) {
	dir := t.TempDir()
	src1 := writeSampleSRT(t, dir, "a.srt", 1)
	src2 := writeSampleSRT(t, dir, "b.srt", 2)

	var active int32
	var maxActive int32
	bothEntered := make(chan struct{})
	var closeOnce sync.Once
	release := make(chan struct{})

	conv := astisub.NewBatchConverter(
		astisub.WithConcurrency(2),
		astisub.WithRenderer(func(ctx context.Context, s *astisub.Subtitles, w io.Writer, target string) error {
			cur := atomic.AddInt32(&active, 1)
			// track max concurrency
			for {
				m := atomic.LoadInt32(&maxActive)
				if cur <= m || atomic.CompareAndSwapInt32(&maxActive, m, cur) {
					break
				}
			}
			if atomic.LoadInt32(&active) == 2 {
				closeOnce.Do(func() { close(bothEntered) })
			}
			select {
			case <-release:
			case <-ctx.Done():
				atomic.AddInt32(&active, -1)
				return ctx.Err()
			}
			atomic.AddInt32(&active, -1)
			return s.WriteToTTML(w)
		}),
	)
	jobs := []astisub.BatchJob{
		{Source: src1, Target: filepath.Join(dir, "a.ttml")},
		{Source: src2, Target: filepath.Join(dir, "b.ttml")},
	}
	done := make(chan []astisub.BatchResult, 1)
	go func() { done <- conv.Convert(context.Background(), jobs) }()

	select {
	case <-bothEntered:
	case <-time.After(10 * time.Second):
		t.Fatalf("jobs did not run concurrently; maxActive=%d", atomic.LoadInt32(&maxActive))
	}
	close(release)

	select {
	case rs := <-done:
		require.Len(t, rs, 2)
		assert.True(t, rs[0].OK && rs[1].OK)
	case <-time.After(10 * time.Second):
		t.Fatal("batch did not finish after release")
	}
	assert.GreaterOrEqual(t, atomic.LoadInt32(&maxActive), int32(2))
}

// ---------------------------------------------------------------------------
// A5: cancellation mid-batch.
//   - already finished job stays successful
//   - in-flight blocked writer fails cancelled and restores its target bytes
//   - not-started jobs are never opened and produce nothing / keep sentinels
// Then re-running the same batch yields the same final state as an
// uninterrupted run.
// ---------------------------------------------------------------------------

func TestConvertBatch_CancelMidBatchThenRerun(t *testing.T) {
	dir := t.TempDir()
	src0 := writeSampleSRT(t, dir, "zero.srt", 0)
	src1 := writeSampleSRT(t, dir, "blocked.srt", 1)
	src2 := writeSampleSRT(t, dir, "never.srt", 2)
	src3 := writeSampleSRT(t, dir, "kept.srt", 3)

	t0 := filepath.Join(dir, "zero.ttml")
	t1 := filepath.Join(dir, "blocked.ttml")
	t2 := filepath.Join(dir, "never.ttml") // intentionally absent before cancel
	t3 := filepath.Join(dir, "kept.ttml")

	sentinelBlocked := []byte("SENTINEL-blocked-original\n")
	sentinelKept := []byte("SENTINEL-kept-original\n")
	writeBytes(t, t1, sentinelBlocked)
	writeBytes(t, t3, sentinelKept)

	var openCount int32
	var enteredOnce sync.Once
	entered := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	conv := astisub.NewBatchConverter(
		astisub.WithConcurrency(1),
		astisub.WithOpen(func(o astisub.Options) (*astisub.Subtitles, error) {
			atomic.AddInt32(&openCount, 1)
			return astisub.Open(o)
		}),
		astisub.WithRenderer(func(ctx context.Context, s *astisub.Subtitles, w io.Writer, target string) error {
			if filepath.Base(target) == "blocked.ttml" {
				enteredOnce.Do(func() { close(entered) })
				<-ctx.Done() // test cancels while this job is "writing"
				return ctx.Err()
			}
			return s.WriteToTTML(w)
		}),
	)
	jobs := []astisub.BatchJob{
		{Source: src0, Target: t0},
		{Source: src1, Target: t1},
		{Source: src2, Target: t2},
		{Source: src3, Target: t3},
	}

	done := make(chan []astisub.BatchResult, 1)
	go func() { done <- conv.Convert(ctx, jobs) }()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("blocked writer never entered")
	}
	cancel()

	var results []astisub.BatchResult
	select {
	case results = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Convert did not return after cancellation")
	}

	require.Len(t, results, 4)
	assertOrderedIndices(t, results)

	// Only job 0 and job 1 were ever opened; jobs 2/3 were not started.
	assert.Equal(t, int32(2), atomic.LoadInt32(&openCount), "not-started inputs must not be parsed")

	// Job 0 succeeded and its output is complete.
	assert.True(t, results[0].OK, "job0: %v", results[0].Err)
	assert.False(t, results[0].Cancelled)
	assert.Contains(t, string(readBytes(t, t0)), "Marker-00")

	// In-flight job failed cancelled and its target kept original bytes.
	assert.False(t, results[1].OK)
	assert.True(t, results[1].Cancelled)
	assert.True(t, errors.Is(results[1].Err, context.Canceled), "got %v", results[1].Err)
	assertBytesEqual(t, sentinelBlocked, readBytes(t, t1))

	// Not-started job 2 produced nothing; job 3 kept its pre-existing bytes.
	assert.False(t, results[2].OK)
	assert.True(t, results[2].Cancelled)
	_, err := os.Stat(t2)
	assert.True(t, os.IsNotExist(err), "unstarted job must not create a target")
	assert.False(t, results[3].OK)
	assert.True(t, results[3].Cancelled)
	assertBytesEqual(t, sentinelKept, readBytes(t, t3))
	assertNoTempResidue(t, dir)

	// Re-run the exact same batch after cancellation: every job succeeds and
	// the on-disk state equals an uninterrupted reference run.
	rerunResults := astisub.ConvertBatch(context.Background(), jobs, astisub.WithConcurrency(1))
	require.Len(t, rerunResults, 4)
	for i, r := range rerunResults {
		require.Truef(t, r.OK, "rerun job %d should succeed: %v", i, r.Err)
	}

	refDir := t.TempDir()
	refJobs := make([]astisub.BatchJob, len(jobs))
	for i, j := range jobs {
		refJobs[i] = astisub.BatchJob{Source: j.Source, Target: filepath.Join(refDir, filepath.Base(j.Target))}
	}
	refResults := astisub.ConvertBatch(context.Background(), refJobs, astisub.WithConcurrency(4))
	require.Len(t, refResults, 4)
	for i, r := range refResults {
		assert.True(t, r.OK)
		assertBytesEqual(t, readBytes(t, r.Target), readBytes(t, rerunResults[i].Target),
			"post-cancel rerun output for job %d differs from uninterrupted run", i)
	}
	assertNoTempResidue(t, dir)
}

// A pre-cancelled context marks every job cancelled without touching disk.
func TestConvertBatch_PreCancelledContext(t *testing.T) {
	dir := t.TempDir()
	src := writeSampleSRT(t, dir, "in.srt", 1)
	target := filepath.Join(dir, "out.ttml")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var opens int32
	results := astisub.ConvertBatch(ctx,
		[]astisub.BatchJob{{Source: src, Target: target}},
		astisub.WithOpen(func(o astisub.Options) (*astisub.Subtitles, error) {
			atomic.AddInt32(&opens, 1)
			return astisub.Open(o)
		}),
	)
	require.Len(t, results, 1)
	assert.False(t, results[0].OK)
	assert.True(t, results[0].Cancelled)
	assert.Error(t, results[0].Err)
	assert.Equal(t, int32(0), opens)
	_, err := os.Stat(target)
	assert.True(t, os.IsNotExist(err))
}

// ---------------------------------------------------------------------------
// A6: repeating the same batch twice yields identical ordered results and
// identical file bytes.
// ---------------------------------------------------------------------------

func TestConvertBatch_RepeatIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	const n = 6
	jobs := make([]astisub.BatchJob, n)
	for i := 0; i < n; i++ {
		src := writeSampleSRT(t, dir, fmt.Sprintf("in-%d.srt", i), i+20)
		ext := ".ttml"
		if i%2 == 1 {
			ext = ".vtt"
		}
		jobs[i] = astisub.BatchJob{Source: src, Target: filepath.Join(dir, fmt.Sprintf("out-%d%s", i, ext))}
	}

	first := astisub.ConvertBatch(context.Background(), jobs, astisub.WithConcurrency(4))
	require.Len(t, first, n)
	for _, r := range first {
		require.True(t, r.OK, "%v", r.Err)
	}
	hash1 := map[string][]byte{}
	for _, j := range jobs {
		hash1[j.Target] = readBytes(t, j.Target)
	}

	second := astisub.ConvertBatch(context.Background(), jobs, astisub.WithConcurrency(4))
	require.Len(t, second, n)

	// All-success DTOs carry no varying fields, so full equality holds.
	assert.True(t, reflect.DeepEqual(first, second),
		"result lists differ:\nfirst=%#v\nsecond=%#v", first, second)
	assert.Equal(t, projectResults(first), projectResults(second))

	for _, j := range jobs {
		assertBytesEqual(t, hash1[j.Target], readBytes(t, j.Target))
	}
}

// ---------------------------------------------------------------------------
// A7: Optimize keeps styles/regions referenced by items or inline fragments
// (and transitive parents / region styles), removes unreferenced ones, and
// the optimized TTML stays reference-consistent when parsed again.
// ---------------------------------------------------------------------------

func assertRichStyleGraphOptimized(t *testing.T, s *astisub.Subtitles) {
	t.Helper()
	require.NotNil(t, s)
	// Referenced direct styles + transitive parent + region style stay.
	for _, id := range []string{"item-style", "parent-style", "span-style", "region-style"} {
		assert.Contains(t, s.Styles, id, "referenced style %q must be kept", id)
	}
	// Unreferenced style is removed.
	assert.NotContains(t, s.Styles, "unused-style")

	assert.Contains(t, s.Regions, "used-region")
	assert.NotContains(t, s.Regions, "unused-region")
}

func TestOptimize_ReferencedStylesAndRegions(t *testing.T) {
	p := writeRichTTML(t, t.TempDir(), "rich.ttml")
	s, err := astisub.Open(astisub.Options{Filename: p})
	require.NoError(t, err)

	// Before optimize everything is present.
	require.Contains(t, s.Styles, "unused-style")
	require.Contains(t, s.Regions, "unused-region")

	s.Optimize()
	assertRichStyleGraphOptimized(t, s)

	// Re-render and re-parse: every emitted style/region reference resolves,
	// otherwise ReadFromTTML returns a "doesn't exist" error.
	var buf bytes.Buffer
	require.NoError(t, s.WriteToTTML(&buf))
	out := buf.String()
	for _, token := range []string{"item-style", "parent-style", "span-style", "region-style", "used-region"} {
		assert.Contains(t, out, token)
	}
	assert.NotContains(t, out, "unused-style")
	assert.NotContains(t, out, "unused-region")

	reparsed, err := astisub.ReadFromTTML(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err, "optimized TTML must remain self-consistent")
	assertRichStyleGraphOptimized(t, reparsed)
}

func TestConvertBatch_WithOptimizeProducesSelfConsistentFile(t *testing.T) {
	dir := t.TempDir()
	src := writeRichTTML(t, dir, "rich.ttml")
	target := filepath.Join(dir, "rich.optimized.ttml")

	results := astisub.ConvertBatch(context.Background(),
		[]astisub.BatchJob{{Source: src, Target: target, Optimize: true}},
		astisub.WithConcurrency(1),
	)
	require.Len(t, results, 1)
	require.True(t, results[0].OK, "%v", results[0].Err)

	out := readBytes(t, target)
	for _, token := range []string{"item-style", "parent-style", "span-style", "region-style", "used-region"} {
		assert.Contains(t, string(out), token)
	}
	assert.NotContains(t, string(out), "unused-style")
	assert.NotContains(t, string(out), "unused-region")

	// Parsing the delivered file validates all internal references.
	reparsed, err := astisub.Open(astisub.Options{Filename: target})
	require.NoError(t, err)
	assertRichStyleGraphOptimized(t, reparsed)
}

// ---------------------------------------------------------------------------
// A8: a failing file elsewhere in the batch must not cause Optimize on a
// successful file to drop styles that file still uses.
// ---------------------------------------------------------------------------

func TestConvertBatch_StyleIsolationWhenOtherFileFails(t *testing.T) {
	dir := t.TempDir()
	srcBad := writeBrokenSRT(t, dir, "broken.srt")
	srcRich := writeRichTTML(t, dir, "rich.ttml")

	badTarget := filepath.Join(dir, "broken.ttml")
	richTarget := filepath.Join(dir, "rich.ttml.out.ttml")
	sentinel := []byte("broken-original-kept\n")
	writeBytes(t, badTarget, sentinel)

	jobs := []astisub.BatchJob{
		{Source: srcBad, Target: badTarget},
		{Source: srcRich, Target: richTarget, Optimize: true},
	}
	results := astisub.ConvertBatch(context.Background(), jobs, astisub.WithConcurrency(2))
	require.Len(t, results, 2)

	assert.False(t, results[0].OK)
	assert.Error(t, results[0].Err)
	assertBytesEqual(t, sentinel, readBytes(t, badTarget))

	require.True(t, results[1].OK, "rich job failed: %v", results[1].Err)
	out := string(readBytes(t, richTarget))

	// Styles referenced by the successful file survive despite the sibling
	// failure and per-file Optimize.
	for _, token := range []string{"item-style", "parent-style", "span-style", "region-style", "used-region"} {
		assert.Contains(t, out, token, "referenced token %q missing", token)
	}
	assert.NotContains(t, out, "unused-style")
	assert.NotContains(t, out, "unused-region")

	reparsed, err := astisub.Open(astisub.Options{Filename: richTarget})
	require.NoError(t, err)
	assertRichStyleGraphOptimized(t, reparsed)

	// A second execution path: batch without optimize, then Optimize the
	// re-opened successful deliverable explicitly. Sibling failure still has
	// no effect.
	richTarget2 := filepath.Join(dir, "rich.plain.ttml")
	jobs2 := []astisub.BatchJob{
		{Source: srcBad, Target: filepath.Join(dir, "broken2.ttml")},
		{Source: srcRich, Target: richTarget2},
	}
	rs2 := astisub.ConvertBatch(context.Background(), jobs2, astisub.WithConcurrency(2))
	require.Len(t, rs2, 2)
	assert.False(t, rs2[0].OK)
	require.True(t, rs2[1].OK)

	s, err := astisub.Open(astisub.Options{Filename: richTarget2})
	require.NoError(t, err)
	s.Optimize()
	assertRichStyleGraphOptimized(t, s)
}

// Empty batch is a valid empty result, and aliases stay usable.
func TestConvertBatch_EmptyAndAliases(t *testing.T) {
	rs := astisub.ConvertBatch(context.Background(), nil)
	assert.NotNil(t, rs)
	assert.Len(t, rs, 0)

	// Alias function and aliased types compile and behave identically.
	var jobs []astisub.ConversionJob
	jobs = append(jobs, astisub.ConversionJob{})
	rs2 := astisub.BatchConvert(context.Background(), jobs)
	require.Len(t, rs2, 1)
	assert.False(t, rs2[0].OK) // empty source/target is a per-job failure

	var _ astisub.ConversionResult = astisub.Result{}
}
