package astisub

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BatchJob is one source subtitle file to convert and the Target path to write.
//
// The format of both Source and Target is inferred from their file extension,
// exactly as Open and Subtitles.Write do (for example ".srt", ".ttml" or
// ".vtt"). A batch pairs each Source with its own Target; several jobs never
// share a Target unless the caller asks for it.
type BatchJob struct {
	// Source is the input subtitle path. It is opened through BatchConverter.Open
	// (by default the existing Open function).
	Source string

	// Target is the output subtitle path. It is written through the existing
	// format-specific WriteTo* chain, via a same-directory temporary file and an
	// atomic rename, so a pre-existing Target is only replaced once the whole
	// conversion for this job has succeeded.
	Target string

	// Options are the options passed to Open. Options.Filename is overwritten
	// with Source.
	Options Options

	// Optimize runs (*Subtitles).Optimize on the parsed document before writing.
	// It is always safe per-job: parsed documents are never shared between jobs.
	Optimize bool
}

// BatchResult is the outcome of one BatchJob. ConvertBatch returns one result
// per input job, at the same index, so results are ordered exactly like the
// inputs and every input appears exactly once.
type BatchResult struct {
	// Index is the position of the corresponding BatchJob in the input slice.
	Index int

	// Source and Target are copied from the matching job for convenience.
	Source string
	Target string

	// OK reports whether the job was parsed, rendered and atomically committed
	// successfully.
	OK bool

	// Cancelled reports whether the job did not complete because the batch
	// context was cancelled (either before it started or while it was writing).
	Cancelled bool

	// Err is the failure cause, wrapped with context. It is nil on success.
	Err error

	// Reason is the human-readable failure reason (Err.Error()), or empty on
	// success. It is safe to compare/display/serialize.
	Reason string
}

// Job and Result are short aliases for BatchJob and BatchResult.
type (
	Job    = BatchJob
	Result = BatchResult
)

// ConversionJob and ConversionResult are descriptive aliases kept for callers
// that prefer the "Conversion*" naming.
type (
	ConversionJob    = BatchJob
	ConversionResult = BatchResult
)

// BatchOption configures a BatchConverter created through ConvertBatch or
// NewBatchConverter.
type BatchOption func(*BatchConverter)

// WithConcurrency sets the maximum number of jobs processed at the same time.
// Values <= 1 make the batch fully sequential, which is useful for
// deterministic cancellation tests. A value <= 0 selects a sensible default
// based on GOMAXPROCS and the number of jobs.
func WithConcurrency(n int) BatchOption {
	return func(c *BatchConverter) { c.Concurrency = n }
}

// WithOptimize makes every job call (*Subtitles).Optimize before writing.
// A job can additionally opt in (or out, by leaving the converter default
// false) through BatchJob.Optimize.
func WithOptimize(optimize bool) BatchOption {
	return func(c *BatchConverter) { c.Optimize = optimize }
}

// WithOpen overrides the parse step. It defaults to Open, the existing
// extension-dispatched entry point. Tests can use it to inject parse failures
// or synchronize with specific jobs.
func WithOpen(open func(o Options) (*Subtitles, error)) BatchOption {
	return func(c *BatchConverter) { c.Open = open }
}

// WithRenderer overrides the rendering step that writes a fully converted
// *Subtitles document into w. It defaults to WriteToWriter, the same
// extension-dispatched chain used by Subtitles.Write
// (WriteToSRT/WriteToTTML/WriteToWebVTT/...).
//
// The writer w is a private temporary file in Target's directory. Writes are
// aborted (and the temporary file removed) as soon as render fails, so tests
// can inject a mid-write failure by wrapping w with a failing io.Writer (or by
// returning an error after writing partial bytes); the pre-existing Target is
// never touched until the atomic rename.
func WithRenderer(render func(ctx context.Context, s *Subtitles, w io.Writer, target string) error) BatchOption {
	return func(c *BatchConverter) { c.Render = render }
}

// BatchConverter runs BatchJobs. The zero value is not usable; create one with
// NewBatchConverter (or call ConvertBatch directly).
//
// All function fields are optional and default to the production parse/write
// chain. They must be safe for concurrent use when Concurrency > 1.
type BatchConverter struct {
	// Concurrency is the maximum number of simultaneously processed jobs.
	Concurrency int

	// Optimize runs (*Subtitles).Optimize on every job when true.
	Optimize bool

	// Open parses one source. Defaults to Open.
	Open func(o Options) (*Subtitles, error)

	// Render writes one document into the job's private temporary writer.
	// Defaults to WriteToWriter, the extension-dispatched WriteTo* chain.
	Render func(ctx context.Context, s *Subtitles, w io.Writer, target string) error
}

// NewBatchConverter builds a BatchConverter with the given options.
func NewBatchConverter(opts ...BatchOption) *BatchConverter {
	c := &BatchConverter{}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// ConvertBatch converts every job and returns the ordered per-job results.
//
// The returned slice always has len(jobs): result i corresponds to jobs[i],
// carries Index == i and each input appears exactly once. A failure on one
// job is captured in its BatchResult and never interrupts the other jobs.
//
// If ctx is cancelled, jobs that have not been handed to a worker are marked
// as cancelled without being opened, in-flight writes are aborted and their
// temporary files removed (their existing targets survive byte-for-byte),
// while already committed jobs remain on disk.
func ConvertBatch(ctx context.Context, jobs []BatchJob, opts ...BatchOption) []BatchResult {
	return NewBatchConverter(opts...).Convert(ctx, jobs)
}

// BatchConvert is an alternative name for ConvertBatch.
func BatchConvert(ctx context.Context, jobs []BatchJob, opts ...BatchOption) []BatchResult {
	return ConvertBatch(ctx, jobs, opts...)
}

// Convert runs the batch with the converter configuration.
func (c *BatchConverter) Convert(ctx context.Context, jobs []BatchJob) []BatchResult {
	if ctx == nil {
		ctx = context.Background()
	}
	results := make([]BatchResult, len(jobs))
	if len(jobs) == 0 {
		return results
	}

	workers := c.Concurrency
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers > len(jobs) {
		workers = len(jobs)
	}
	if workers < 1 {
		workers = 1
	}

	type assignment struct {
		index int
		job   BatchJob
	}

	// Unbuffered assignments give the feeder back-pressure: an assignment has
	// been handed to a worker only when the send completes. On ctx
	// cancellation the feeder can therefore mark every un-handed index as
	// cancelled without risking double processing.
	assignments := make(chan assignment)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range assignments {
				results[a.index] = c.processOne(ctx, a.job, a.index)
			}
		}()
	}

	firstUnassigned := len(jobs)
feed:
	for i, job := range jobs {
		select {
		case assignments <- assignment{index: i, job: job}:
			// Ownership of index i moved to a worker.
		case <-ctx.Done():
			firstUnassigned = i
			break feed
		}
	}
	close(assignments)
	wg.Wait()

	// These indices were never handed to a worker, so no other goroutine can
	// write them. Record them exactly once as cancelled.
	for i := firstUnassigned; i < len(jobs); i++ {
		err := fmt.Errorf("astisub: batch job %d (%q -> %q) not started: %w",
			i, jobs[i].Source, jobs[i].Target, ctx.Err())
		results[i] = BatchResult{
			Index:     i,
			Source:    jobs[i].Source,
			Target:    jobs[i].Target,
			Cancelled: true,
			Err:       err,
			Reason:    err.Error(),
		}
	}
	return results
}

// processOne handles parse -> optimize -> atomic write for one job.
func (c *BatchConverter) processOne(ctx context.Context, job BatchJob, index int) BatchResult {
	r := BatchResult{Index: index, Source: job.Source, Target: job.Target}
	fail := func(err error) BatchResult {
		r.OK = false
		r.Err = err
		if err != nil {
			r.Reason = err.Error()
		}
		// Any context cancellation/deadline observed around the failure
		// classifies the result as cancelled.
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.Cancelled = true
		}
		return r
	}

	// Gate before any work so a job released from the queue after cancellation
	// never touches the file system through Open.
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("astisub: batch job %d (%q -> %q) cancelled before start: %w",
			index, job.Source, job.Target, err))
	}

	if strings.TrimSpace(job.Source) == "" || strings.TrimSpace(job.Target) == "" {
		return fail(errors.New("astisub: batch job requires both a source and a target path"))
	}

	// Parse through the existing Open chain. Every call returns an independent
	// *Subtitles graph, which is what keeps Optimize decisions isolated across
	// concurrent jobs.
	open := c.Open
	if open == nil {
		open = Open
	}
	o := job.Options
	o.Filename = job.Source
	sub, err := open(o)
	if err != nil {
		return fail(fmt.Errorf("astisub: batch job %d: parsing %q failed: %w", index, job.Source, err))
	}

	// Optimize only this job's own document.
	if c.Optimize || job.Optimize {
		sub.Optimize()
	}

	// Re-check cancellation before the commit-capable write.
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("astisub: batch job %d (%q -> %q) cancelled before write: %w",
			index, job.Source, job.Target, err))
	}

	if err := c.writeAtomic(ctx, job, sub); err != nil {
		return fail(fmt.Errorf("astisub: batch job %d: writing %q failed: %w", index, job.Target, err))
	}

	r.OK = true
	return r
}

// writeAtomic renders sub into a unique temporary file in Target's directory
// and only then renames it over Target. Any failure removes the temporary
// file and leaves a pre-existing Target untouched.
func (c *BatchConverter) writeAtomic(ctx context.Context, job BatchJob, sub *Subtitles) error {
	target := job.Target
	render := c.Render
	if render == nil {
		render = defaultBatchRenderer
	}
	return AtomicWrite(target, func(tmp io.Writer) error {
		// Make cancellation observable even inside the stock WriteTo* chain:
		// the next write after ctx cancellation fails with the context error.
		writer := &contextWriter{ctx: ctx, w: tmp}
		if err := render(ctx, sub, writer, target); err != nil {
			return err
		}
		// Rendering may have succeeded just before cancellation; abort the
		// commit in that case.
		return ctx.Err()
	})
}

// WriteFileAtomic renders sub with the format implied by filename and
// atomically replaces filename. It is the single-file primitive used by the
// batch entry and is safe to call repeatedly.
func WriteFileAtomic(filename string, sub *Subtitles) error {
	if sub == nil {
		return errors.New("astisub: cannot write nil subtitles")
	}
	return AtomicWrite(filename, func(w io.Writer) error {
		return WriteToWriter(sub, w, filename)
	})
}

// AtomicWrite writes a file transactionally: fn writes the complete content
// into a private temporary file located in filename's directory, and filename
// is replaced by that file (atomic rename) only when fn succeeds and closes
// cleanly.
//
// If fn returns an error, writing fails, or the rename fails, the temporary
// file is removed and any pre-existing filename keeps its original bytes.
// Existing permission bits are preserved; new files use 0666 subject to the
// process umask, exactly like os.Create.
//
// AtomicWrite is also the intended seam for injected write failures in tests:
// pass an fn (or an io.Writer wrapper) that fails after partial writes and
// assert the original file bytes are unchanged.
func AtomicWrite(filename string, fn func(w io.Writer) error) error {
	if fn == nil {
		return errors.New("astisub: AtomicWrite requires a non-nil write function")
	}

	// Preserve the permission bits of an existing target.
	var mode os.FileMode
	if fi, err := os.Stat(filename); err == nil && !fi.IsDir() {
		mode = fi.Mode().Perm()
	}

	f, temporaryPath, err := openTemporaryForTarget(filename)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		_ = f.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if mode != 0 {
		_ = f.Chmod(mode)
	}

	if err = fn(f); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("astisub: closing temporary file for %q failed: %w", filename, err)
	}
	if err = renameAtomically(temporaryPath, filename); err != nil {
		return fmt.Errorf("astisub: replacing %q failed: %w", filename, err)
	}
	committed = true
	return nil
}

// defaultBatchRenderer dispatches to the same format-specific writers as
// Subtitles.Write, using the target filename extension.
func defaultBatchRenderer(_ context.Context, s *Subtitles, w io.Writer, target string) error {
	return WriteToWriter(s, w, target)
}

// WriteToWriter writes s using the format implied by filename's extension. It
// is the shared extension dispatch used by Subtitles.Write and the batch
// renderer, so batch conversions stay on the existing parse/write chain.
func WriteToWriter(s *Subtitles, w io.Writer, filename string) error {
	switch filepath.Ext(strings.ToLower(filename)) {
	case ".srt":
		return s.WriteToSRT(w)
	case ".ssa", ".ass":
		return s.WriteToSSA(w)
	case ".stl":
		return s.WriteToSTL(w)
	case ".ttml":
		return s.WriteToTTML(w)
	case ".vtt":
		return s.WriteToWebVTT(w)
	default:
		return ErrInvalidExtension
	}
}

// contextWriter fails writes once ctx is cancelled.
type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (cw *contextWriter) Write(p []byte) (int, error) {
	if cw.ctx != nil {
		if err := cw.ctx.Err(); err != nil {
			return 0, err
		}
	}
	return cw.w.Write(p)
}

// tempFileSequence keeps temporary names unique within the process.
var tempFileSequence uint64

// openTemporaryForTarget creates a unique O_EXCL temporary file in target's
// directory without following any fixed name, so cancelled or crashed runs
// cannot leave a well-known residue that affects the next execution.
func openTemporaryForTarget(target string) (*os.File, string, error) {
	dir := filepath.Dir(target)
	base := filepath.Base(target)
	if dir == "" {
		dir = "."
	}
	for attempt := 0; attempt < 128; attempt++ {
		seq := atomic.AddUint64(&tempFileSequence, 1)
		var randomSuffix [4]byte
		_, _ = rand.Read(randomSuffix[:])
		name := fmt.Sprintf(".%s.astisub-tmp-%d-%x", base, seq, randomSuffix)
		p := filepath.Join(dir, name)
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
		if err == nil {
			return f, p, nil
		}
		if os.IsExist(err) {
			continue
		}
		return nil, "", fmt.Errorf("astisub: creating temporary file %q failed: %w", p, err)
	}
	return nil, "", errors.New("astisub: could not allocate a unique temporary file")
}

// renameAtomically renames old over new, retrying briefly on Windows where a
// freshly closed file can transiently be locked by antivirus or the indexer.
func renameAtomically(old, new string) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = os.Rename(old, new); err == nil {
			return nil
		}
		if runtime.GOOS != "windows" {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	return err
}
