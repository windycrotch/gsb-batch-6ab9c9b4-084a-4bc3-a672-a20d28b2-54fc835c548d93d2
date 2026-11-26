package spireav

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Progress struct {
	Frame      int64
	FPS        float64
	Bitrate    int64
	TotalSize  int64
	OutTime    string
	DupFrames  int64
	DropFrames int64
	Speed      float64
	Done       bool
	Started    time.Time
	Elapsed    time.Duration
}

type Runner interface {
	Run(ctx context.Context) error
	CommandString() (string, error)
}

type Option func(*implRunner)

func WithProgressCallback(callback func(Progress)) Option {
	return func(r *implRunner) {
		r.progressCallback = callback
	}
}

func WithSysProcAttr(attr *syscall.SysProcAttr) Option {
	return func(r *implRunner) {
		r.sysProcAttr = attr
	}
}

func NewRunner(runnable Runnable, opts ...Option) Runner {
	runner := &implRunner{
		runnable: runnable,
	}
	for _, opt := range opts {
		opt(runner)
	}
	return runner
}

type implRunner struct {
	runnable         Runnable
	progressCallback func(Progress)
	sysProcAttr      *syscall.SysProcAttr
}

// CommandString is a utility function that gets the FFmpeg command string that is run by this process
func (p *implRunner) CommandString() (string, error) {
	args, err := p.runnable.RunnableArgs()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s %s\n",
		FfmpegPath,
		strings.Join(args, " "),
	), nil
}

// Run executes the transcoding process and blocks the calling thread until the process is completed or failed.
//
// Progress callbacks (if configured) are delivered sequentially, one per FFmpeg progress block, in
// the order that FFmpeg emitted them. The Elapsed field never decreases. On a successful run, the
// final callback has Done set to true, and Run does not return until every callback has returned.
//
// If the context is canceled, Run returns the context's error. No new progress callback is invoked
// once the cancellation has been observed, and Run does not return until any in-flight callback has
// returned. Cancellation takes precedence over the completed-run behavior above: a run whose context
// was canceled before Run returned does not deliver the final done callback.
//
// If FFmpeg itself exits with a non-zero status, Run returns an *FfmpegError containing the exit
// code and the collected stderr output, and no further progress callbacks are invoked.
func (p *implRunner) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// Generate the FFmpeg arguments
	args, err := p.runnable.RunnableArgs()
	if err != nil {
		return fmt.Errorf("generating ffmpeg args: %w", err)
	}

	// Stderr will contain error messages ONLY.
	// Stdout will contain a streaming progress report.
	// We create the pipes ourselves (instead of using StdoutPipe/StderrPipe) so that closing
	// the write ends reliably signals EOF to the readers when the child process exits.
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("creating stdout pipe: %w", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		stdoutR.Close()
		stdoutW.Close()
		return fmt.Errorf("creating stderr pipe: %w", err)
	}

	// Create the command
	cmd := exec.CommandContext(ctx, FfmpegPath, args...)
	cmd.SysProcAttr = p.sysProcAttr
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	delivery := newProgressDelivery(ctx, p.progressCallback)

	// Capture and buffer stderr, and parse the stdout progress report. Both readers run
	// until EOF and are always joined before Run returns, so no goroutine can still be
	// delivering callbacks after Run has returned.
	var (
		wg        sync.WaitGroup
		stderrBuf []byte
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		stderrBuf, _ = io.ReadAll(stderrR)
	}()
	go func() {
		defer wg.Done()
		if p.progressCallback != nil {
			p.reportFFmpegProgress(stdoutR, delivery)
		} else {
			io.Copy(io.Discard, stdoutR)
		}
	}()

	// Start running the command in the background
	startErr := cmd.Start()

	// The child process owns duplicates of the write ends now. Close ours so that the
	// readers observe EOF as soon as the process (and any of its children) exits.
	stdoutW.Close()
	stderrW.Close()

	if startErr != nil {
		wg.Wait()
		stdoutR.Close()
		stderrR.Close()

		// If the context triggered the failure to start, surface the context's error.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("starting ffmpeg process: %w", startErr)
	}

	// Wait for the process to end
	waitErr := cmd.Wait()

	// Decide how the run will end before waiting on the readers, so we can stop the
	// delivery of callbacks as early as possible. close() also waits for an in-flight
	// callback to return.
	if ctx.Err() != nil || waitErr != nil {
		delivery.close()
	}

	// Wait for the readers to finish (this also waits for every callback to return).
	wg.Wait()
	stdoutR.Close()
	stderrR.Close()

	// If the context triggered the process to be killed, we want to see the context's
	// error instead of the process's error.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	// If the process errored, return the error
	if waitErr != nil {
		exitCode := -1
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}
		return &FfmpegError{
			ExitCode: exitCode,
			Logs:     strings.Split(strings.TrimSpace(string(stderrBuf)), "\n"),
			child:    waitErr,
		}
	}

	// The process ended successfully. Make sure the caller always observes a final
	// callback marked as done, even if FFmpeg did not emit a "progress=end" block.
	delivery.finishSuccess()
	return nil
}

// reportFFmpegProgress reads FFmpeg's "-progress pipe:1" output and delivers a progress
// callback for every progress block, in the order the blocks appear in the output.
//
// The parser is resilient to output that is split into arbitrary chunks: lines are
// reassembled before parsing. Lines that cannot be understood are ignored, and a
// progress block is never delivered more than once.
func (p *implRunner) reportFFmpegProgress(processOutput io.Reader, delivery *progressDelivery) {
	reader := bufio.NewReader(processOutput)

	var (
		progress Progress
		// blockHasData tracks whether any known field has been parsed since the last
		// delivered block. It is used to suppress duplicated "progress" keys that
		// belong to an already-delivered block.
		blockHasData = true
		delivered    bool
		lastValue    string
	)

	handleLine := func(line string) {
		line = strings.TrimSpace(line)
		if line == "" {
			return
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			// Not a key/value line: safely ignore it.
			return
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		// Any recognized field marks this block as containing fresh data, even if the
		// value itself cannot be parsed (e.g. "bitrate=N/A").
		if isKnownProgressKey(key) {
			blockHasData = true
		}

		switch key {
		case "frame":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				progress.Frame = v
			}
		case "fps":
			if v, err := strconv.ParseFloat(value, 64); err == nil {
				progress.FPS = v
			}
		case "bitrate":
			if v, err := strconv.ParseFloat(strings.TrimSuffix(value, "kbits/s"), 64); err == nil {
				progress.Bitrate = int64(v * 1000)
			}
		case "total_size":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				progress.TotalSize = v
			}
		case "out_time":
			progress.OutTime = value
		case "dup_frames":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				progress.DupFrames = v
			}
		case "drop_frames":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				progress.DropFrames = v
			}
		case "speed":
			if v, err := strconv.ParseFloat(strings.TrimSuffix(value, "x"), 64); err == nil {
				progress.Speed = v
			}
		case "progress":
			progress.Done = (value == "end")
			if delivered && !blockHasData && lastValue == value {
				// Duplicate progress key for a block that has already been delivered.
				// Report the block only once.
				return
			}
			delivery.deliver(progress)
			delivered = true
			blockHasData = false
			lastValue = value
		}
	}

	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			handleLine(line)
		}
		if err != nil {
			// io.EOF (or a read error) ends the stream. A trailing fragment without a
			// newline has already been handled above.
			return
		}
	}
}

// isKnownProgressKey reports whether key is one of the fields FFmpeg emits in a
// "-progress" block.
func isKnownProgressKey(key string) bool {
	switch key {
	case "frame", "fps", "bitrate", "total_size", "out_time",
		"out_time_us", "out_time_ms", "dup_frames", "drop_frames", "speed":
		return true
	default:
		return false
	}
}

// progressDelivery serializes progress callbacks, stamps the elapsed time, and gates
// delivery so that no callback runs after the delivery has been closed.
//
// The callback is invoked while the mutex is held. This is what makes close() a full
// barrier: once close() returns, no callback is running and no further callback will
// be started.
type progressDelivery struct {
	mu       sync.Mutex
	closed   bool
	ctx      context.Context
	callback func(Progress)

	started   time.Time
	last      Progress
	delivered bool
	done      bool
}

func newProgressDelivery(ctx context.Context, callback func(Progress)) *progressDelivery {
	return &progressDelivery{
		ctx:      ctx,
		callback: callback,
		started:  time.Now(),
	}
}

// deliver reports the progress block to the callback, unless delivery has been closed,
// the context has been canceled, or no callback is configured. Elapsed is measured from
// the start of the run and is clamped so that it never decreases, even if the wall clock
// jumps.
func (d *progressDelivery) deliver(progress Progress) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.done || d.callback == nil || d.ctx.Err() != nil {
		return
	}

	progress.Started = d.started
	progress.Elapsed = time.Since(d.started)
	if d.delivered && progress.Elapsed < d.last.Elapsed {
		progress.Elapsed = d.last.Elapsed
	}
	if progress.Done {
		d.done = true
	}

	d.last = progress
	d.delivered = true
	d.callback(progress)
}

// finishSuccess delivers a final callback marked as done if FFmpeg never reported the
// end of the run itself. It must only be called on the successful path.
func (d *progressDelivery) finishSuccess() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.callback == nil || d.done {
		return
	}

	progress := d.last
	if !d.delivered {
		progress = Progress{}
	}
	progress.Started = d.started
	progress.Done = true
	progress.Elapsed = time.Since(d.started)
	if d.delivered && progress.Elapsed < d.last.Elapsed {
		progress.Elapsed = d.last.Elapsed
	}

	d.last = progress
	d.delivered = true
	d.done = true
	d.callback(progress)
}

// close prevents any further callback from starting, and returns only after an
// in-flight callback (if any) has returned.
func (d *progressDelivery) close() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
}
