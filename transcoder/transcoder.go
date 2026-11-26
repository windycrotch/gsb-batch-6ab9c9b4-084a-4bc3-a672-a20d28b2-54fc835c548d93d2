package transcoder

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xfrr/goffmpeg"
	"github.com/xfrr/goffmpeg/media"
)

const (
	// stopTimeout bounds how long Stop waits for ffmpeg to honor a graceful
	// quit before the process is killed.
	stopTimeout = 5 * time.Second
	// killTimeout bounds how long Stop waits for the process to disappear
	// after a kill.
	killTimeout = 2 * time.Second
	// drainGrace is how long the progress reader is given to flush lines
	// that were already produced when the process exits on its own.
	drainGrace = 2 * time.Second
	// readerExitGrace bounds the wait for the progress reader after its pipe
	// has been closed; the reader must not outlive the run.
	readerExitGrace = 2 * time.Second
)

// Transcoder Main struct
type Transcoder struct {
	stdErrPipe         io.ReadCloser
	stdStdinPipe       io.WriteCloser
	process            *exec.Cmd
	mediafile          *media.File
	configuration      goffmpeg.Configuration
	whiteListProtocols []string

	// mu guards run and the process/pipe fields. Every call to Run installs
	// a new *run, and all the state that belongs to one transcoding
	// (process, pipes, progress channel) lives on that run instead of on
	// the Transcoder, so a late reader from a previous task can never
	// observe the current one.
	mu  sync.Mutex
	run *run
}

// run is the entire lifecycle of a single transcoding invocation.
type run struct {
	proc    *exec.Cmd
	file    *media.File
	command string
	outb    *bytes.Buffer

	// durationSec is the input duration captured when the run started. It is
	// snapshotted so a later mutation of the shared media file (for example
	// setting up the next task) cannot change how this run's progress is
	// computed.
	durationSec float64

	stdin       io.WriteCloser
	stderrRead  *os.File
	stderrWrite *os.File

	progressEnabled bool
	progress        chan Progress
	readerDone      chan struct{}

	// stopCh is closed as soon as a stop is requested (and again while
	// tearing down) so the progress reader stops publishing immediately.
	stopCh   chan struct{}
	stopOnce sync.Once

	finishedCh chan struct{}
	startDone  chan struct{}
	done       chan error

	mu            sync.Mutex
	startErr      error
	stopping      bool
	stoppedByUser bool
	finished      bool
}

// SetProcessStderrPipe Set the STDERR pipe
func (t *Transcoder) SetProcessStderrPipe(v io.ReadCloser) {
	t.mu.Lock()
	t.stdErrPipe = v
	t.mu.Unlock()
}

// SetProcessStdinPipe Set the STDIN pipe
func (t *Transcoder) SetProcessStdinPipe(v io.WriteCloser) {
	t.mu.Lock()
	t.stdStdinPipe = v
	t.mu.Unlock()
}

// SetProcess Set the transcoding process
func (t *Transcoder) SetProcess(cmd *exec.Cmd) {
	t.mu.Lock()
	t.process = cmd
	t.mu.Unlock()
}

// SetMediaFile Set the media file
func (t *Transcoder) SetMediaFile(v *media.File) {
	t.mediafile = v
}

// SetConfiguration Set the transcoding configuration
func (t *Transcoder) SetConfiguration(v goffmpeg.Configuration) {
	t.configuration = v
}

func (t *Transcoder) SetWhiteListProtocols(availableProtocols []string) {
	t.whiteListProtocols = availableProtocols
}

// Process Get transcoding process
func (t *Transcoder) Process() *exec.Cmd {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.process
}

// MediaFile Get the ttranscoding media file.
func (t *Transcoder) MediaFile() *media.File {
	return t.mediafile
}

// FFmpegExec Get FFmpeg Bin path
func (t *Transcoder) FFmpegExec() string {
	return t.configuration.FFmpegBinPath()
}

// FFprobeExec Get FFprobe Bin path
func (t *Transcoder) FFprobeExec() string {
	return t.configuration.FFprobeBinPath()
}

// GetCommand Build and get command
func (t *Transcoder) GetCommand() []string {
	media := t.mediafile
	rcommand := append([]string{"-y"}, media.ToStrCommand()...)

	if t.whiteListProtocols != nil {
		rcommand = append([]string{"-protocol_whitelist", strings.Join(t.whiteListProtocols, ",")}, rcommand...)
	}

	return rcommand
}

// InitializeEmptyTranscoder initializes the fields necessary for a blank transcoder
func (t *Transcoder) InitializeEmptyTranscoder() error {
	var Metadata media.Metadata

	var err error
	cfg := t.configuration
	if len(cfg.FFmpegBinPath()) == 0 || len(cfg.FFprobeBinPath()) == 0 {
		cfg, err = goffmpeg.Configure(context.Background())
		if err != nil {
			return err
		}
	}
	// Set new File
	MediaFile := new(media.File)
	MediaFile.SetMetadata(Metadata)

	// Set transcoder configuration
	t.SetMediaFile(MediaFile)
	t.SetConfiguration(cfg)
	return nil
}

// SetInputPath sets the input path for transcoding
func (t *Transcoder) SetInputPath(inputPath string) error {
	if t.mediafile.InputPipe() {
		return errors.New("cannot set an input path when an input pipe command has been set")
	}
	t.mediafile.SetInputPath(inputPath)
	return nil
}

// SetOutputPath sets the output path for transcoding
func (t *Transcoder) SetOutputPath(inputPath string) error {
	if t.mediafile.OutputPipe() {
		return errors.New("cannot set an input path when an input pipe command has been set")
	}
	t.mediafile.SetOutputPath(inputPath)
	return nil
}

// CreateInputPipe creates an input pipe for the transcoding process
func (t *Transcoder) CreateInputPipe() (*io.PipeWriter, error) {
	if t.mediafile.InputPath() != "" {
		return nil, errors.New("cannot set an input pipe when an input path exists")
	}
	inputPipeReader, inputPipeWriter := io.Pipe()
	t.mediafile.SetInputPipe(true)
	t.mediafile.SetInputPipeReader(inputPipeReader)
	t.mediafile.SetInputPipeWriter(inputPipeWriter)
	return inputPipeWriter, nil
}

// CreateOutputPipe creates an output pipe for the transcoding process
func (t *Transcoder) CreateOutputPipe(containerFormat string) (*io.PipeReader, error) {
	if t.mediafile.OutputPath() != "" {
		return nil, errors.New("cannot set an output pipe when an output path exists")
	}
	t.mediafile.SetOutputFormat(containerFormat)

	t.mediafile.SetMovFlags("frag_keyframe")
	outputPipeReader, outputPipeWriter := io.Pipe()
	t.mediafile.SetOutputPipe(true)
	t.mediafile.SetOutputPipeReader(outputPipeReader)
	t.mediafile.SetOutputPipeWriter(outputPipeWriter)
	return outputPipeReader, nil
}

// Initialize Init the transcoding process
func (t *Transcoder) Initialize(inputPath string, outputPath string) error {
	var err error
	var outb, errb bytes.Buffer
	var Metadata media.Metadata

	cfg := t.configuration

	if len(cfg.FFmpegBinPath()) == 0 || len(cfg.FFprobeBinPath()) == 0 {
		cfg, err = goffmpeg.Configure(context.Background())
		if err != nil {
			return err
		}
	}

	if inputPath == "" {
		return errors.New("error on transcoder.Initialize: inputPath missing")
	}

	command := []string{"-i", inputPath, "-print_format", "json", "-show_format", "-show_streams", "-show_error"}

	if t.whiteListProtocols != nil {
		command = append([]string{"-protocol_whitelist", strings.Join(t.whiteListProtocols, ",")}, command...)
	}

	cmd := exec.Command(cfg.FFprobeBinPath(), command...)
	cmd.Stdout = &outb
	cmd.Stderr = &errb

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("error executing (%s) | error: %s | message: %s %s", command, err, outb.String(), errb.String())
	}

	if err = json.Unmarshal(outb.Bytes(), &Metadata); err != nil {
		return err
	}

	// Set new File
	MediaFile := new(media.File)
	MediaFile.SetMetadata(Metadata)
	MediaFile.SetInputPath(inputPath)
	MediaFile.SetOutputPath(outputPath)

	// Set transcoder configuration
	t.SetMediaFile(MediaFile)
	t.SetConfiguration(cfg)

	return nil

}

// Run Starts the transcoding process.
//
// Every call creates an independent run: its own process, its own stderr
// pipe and its own progress channel. Starting a transcoder whose previous
// run is still alive first tears that run down completely, so progress from
// the old task can never be observed as progress of the new one.
func (t *Transcoder) Run(progress bool) <-chan error {
	return t.runWith(t.configuration.FFmpegBinPath(), progress)
}

// runWith starts a run using bin as the ffmpeg executable. It exists so the
// start-failure path can be exercised with a real (but missing) binary.
func (t *Transcoder) runWith(bin string, progress bool) <-chan error {
	// Terminate and fully clean up a still-running previous run before
	// installing the new one.
	t.mu.Lock()
	previous := t.run
	t.mu.Unlock()
	if previous != nil {
		previous.stop()
	}

	command := t.GetCommand()

	if !progress {
		command = append([]string{"-nostats", "-loglevel", "0"}, command...)
	}

	proc := exec.Command(bin, command...)

	r := &run{
		proc:            proc,
		file:            t.mediafile,
		command:         fmt.Sprintf("%s %s", bin, strings.Join(command, " ")),
		outb:            &bytes.Buffer{},
		progressEnabled: progress,
		readerDone:      make(chan struct{}),
		stopCh:          make(chan struct{}),
		finishedCh:      make(chan struct{}),
		startDone:       make(chan struct{}),
		done:            make(chan error, 1),
	}
	if t.mediafile != nil {
		r.durationSec, _ = strconv.ParseFloat(t.mediafile.Metadata().Format.Duration, 64)
	}

	// A buffered channel lets the "no progress" sentinel be delivered
	// without requiring a consumer, matching the historical behaviour of
	// Output() when progress was disabled.
	if progress {
		r.progress = make(chan Progress)
	} else {
		r.progress = make(chan Progress, 1)
	}

	// Use an explicit os.Pipe rather than exec.Cmd.StderrPipe so that Wait
	// never closes the read end underneath the progress reader. Cleanup is
	// then explicit and identical for every exit path.
	if progress {
		stderrRead, stderrWrite, err := os.Pipe()
		if err != nil {
			fmt.Println("Progress not available: " + err.Error())
			r.progressEnabled = false
		} else {
			r.stderrRead = stderrRead
			r.stderrWrite = stderrWrite
			proc.Stderr = stderrWrite
		}
	}

	// Set the stdinPipe in case we need to stop the transcoding
	stdin, err := proc.StdinPipe()
	if nil != err {
		fmt.Println("Stdin not available: " + err.Error())
	} else {
		r.stdin = stdin
	}

	// If the user has requested progress, we send it to them on a Buffer
	if progress {
		proc.Stdout = r.outb
	}

	// If an input pipe has been set, we set it as stdin for the transcoding
	if t.mediafile.InputPipe() {
		proc.Stdin = t.mediafile.InputPipeReader()
	}

	// If an output pipe has been set, we set it as stdout for the transcoding
	if t.mediafile.OutputPipe() {
		proc.Stdout = t.mediafile.OutputPipeWriter()
	}

	// The progress reader starts before the process so it is already
	// waiting in Read when the first statistics line arrives.
	go r.readProgress()

	// Publish the run before starting the process. A Stop racing with Run
	// then always finds this run; it waits for startDone before deciding
	// whether there is a process to interrupt.
	t.mu.Lock()
	t.run = r
	t.process = proc
	t.mu.Unlock()

	startErr := proc.Start()

	r.mu.Lock()
	r.startErr = startErr
	r.mu.Unlock()
	close(r.startDone)

	// The parent must let go of the write end now that the child owns a
	// copy; otherwise the reader never sees EOF when ffmpeg exits.
	if r.stderrWrite != nil {
		r.stderrWrite.Close()
	}

	go func() {
		var waitErr error
		if startErr == nil {
			waitErr = proc.Wait()
		}
		t.finishRun(r, startErr, waitErr)
	}()

	return r.done
}

// Stop Ends the transcoding process.
//
// Stopping a transcoder that was never started, has already finished or is
// already stopping is safe: it neither blocks indefinitely nor tears the
// same run down twice. When it returns for a running task, that task's
// progress channel is closed and no further progress is produced for it.
func (t *Transcoder) Stop() error {
	t.mu.Lock()
	r := t.run
	process := t.process
	stdin := t.stdStdinPipe
	t.mu.Unlock()

	if r != nil {
		r.stop()
		return nil
	}

	// Legacy manual wiring: honour a process installed with SetProcess.
	if process != nil && stdin != nil {
		_, _ = stdin.Write([]byte("q\n"))
	}
	return nil
}

// Output Returns the transcoding progress channel.
//
// The channel belongs to the most recent run. Once that run is stopped or
// finishes, the channel is closed; a new Run installs a fresh channel that
// only carries the new task's progress.
func (t *Transcoder) Output() <-chan Progress {
	t.mu.Lock()
	r := t.run
	legacyPipe := t.stdErrPipe
	t.mu.Unlock()

	var legacyDuration float64
	if t.mediafile != nil {
		legacyDuration, _ = strconv.ParseFloat(t.mediafile.Metadata().Format.Duration, 64)
	}

	if r != nil {
		return r.progress
	}

	out := make(chan Progress)
	go func() {
		defer close(out)
		if legacyPipe == nil {
			out <- Progress{}
			return
		}
		defer legacyPipe.Close()
		scanStderr(out, legacyPipe, legacyDuration, nil)
	}()

	return out
}

func (r *run) signalStop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

// stop requests a graceful shutdown and waits for the run to be fully torn
// down. It is idempotent and safe to call from any state.
func (r *run) stop() {
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	if r.stopping {
		r.mu.Unlock()
		// Another caller is already tearing this run down; wait for it, but
		// never block past the bounded shutdown window.
		select {
		case <-r.finishedCh:
		case <-time.After(stopTimeout + killTimeout):
		}
		return
	}
	r.stopping = true
	r.stoppedByUser = true
	r.mu.Unlock()

	// Stop publishing progress and stop reading stderr before touching the
	// process: no line parsed after the stop request may reach the
	// consumer.
	r.signalStop()

	// A stop can race with Run: wait until Start has decided whether there
	// is a process to talk to.
	<-r.startDone
	if r.startErr != nil {
		// The start-failure teardown path will finish the run.
		select {
		case <-r.finishedCh:
		case <-time.After(stopTimeout):
		}
		return
	}

	if r.stdin != nil {
		_, _ = r.stdin.Write([]byte("q\n"))
	}

	select {
	case <-r.finishedCh:
	case <-time.After(stopTimeout):
		if r.proc != nil && r.proc.Process != nil {
			_ = r.proc.Process.Kill()
		}
		select {
		case <-r.finishedCh:
		case <-time.After(killTimeout):
		}
	}
}

// readProgress consumes ffmpeg's statistics and owns the progress channel:
// it is the only goroutine that closes it.
//
// The reader also owns the read end of the stderr pipe and is the only
// goroutine that closes it. Closing a pipe end that another goroutine is
// blocked reading on is not reliably safe on Windows, so the teardown paths
// instead make the read finish on its own: ffmpeg exiting (or its stderr
// write end being closed) produces EOF. A stop only flips the stop signal and
// interrupts the process, which the reader observes as EOF.
func (r *run) readProgress() {
	defer close(r.readerDone)
	defer close(r.progress)
	if r.stderrRead != nil {
		defer r.stderrRead.Close()
	}

	if !r.progressEnabled || r.stderrRead == nil {
		select {
		case <-r.stopCh:
		case r.progress <- Progress{}:
		}
		return
	}

	scanStderr(r.progress, r.stderrRead, r.durationSec, r.stopCh)
}

// scanStderr parses progress lines from pipe until EOF, a read error, or
// (when stop is non-nil) a stop signal. A line that has not been sent when
// stop fires is dropped, so nothing parsed after a stop reaches the consumer.
func scanStderr(out chan<- Progress, pipe io.Reader, durationSec float64, stop <-chan struct{}) {
	scanner := bufio.NewScanner(pipe)
	scanner.Split(scanProgressLines)
	buf := make([]byte, 2)
	scanner.Buffer(buf, bufio.MaxScanTokenSize)

	for scanner.Scan() {
		if stopped(stop) {
			return
		}

		progress, ok := parseProgressLine(scanner.Text(), durationSec)
		if !ok {
			continue
		}

		if stop == nil {
			out <- progress
			continue
		}

		// Re-check immediately before handing the value over: once a stop
		// has been requested the reader must not publish anything more.
		if stopped(stop) {
			return
		}
		select {
		case <-stop:
			return
		case out <- progress:
		}
	}
}

func stopped(stop <-chan struct{}) bool {
	if stop == nil {
		return false
	}
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// finishRun performs the single teardown path shared by normal completion,
// user stop and process start failure. It runs exactly once per run.
func (t *Transcoder) finishRun(r *run, startErr, waitErr error) {
	r.mu.Lock()
	stoppedByUser := r.stoppedByUser
	r.mu.Unlock()

	if r.stdin != nil {
		_ = r.stdin.Close()
	}

	// Closing the write end is what lets the reader see EOF now that the
	// process is gone (or was never started). It also silences ffmpeg's
	// stderr if the process somehow survived.
	if r.stderrWrite != nil {
		_ = r.stderrWrite.Close()
	}

	// Give the reader a brief chance to flush progress ffmpeg wrote before
	// exiting on its own. On a user stop the reader must be silenced
	// immediately instead.
	if !stoppedByUser {
		select {
		case <-r.readerDone:
		case <-time.After(drainGrace):
		}
	}

	// The stop signal keeps the reader from publishing anything it happened
	// to parse after a stop. The read end is closed by the reader itself,
	// never from here.
	r.signalStop()
	select {
	case <-r.readerDone:
	case <-time.After(readerExitGrace):
	}

	closeMediaPipes(r.file)

	var err error
	switch {
	case startErr != nil:
		err = fmt.Errorf("failed start ffmpeg (%s) with %s, message %s %s", r.command, startErr, r.outb.String(), "")
	case stoppedByUser:
		// A user-requested stop is a clean outcome even though ffmpeg may
		// exit non-zero after being interrupted.
		err = nil
	case waitErr != nil:
		err = fmt.Errorf("failed finish ffmpeg (%s) with %s message %s %s", r.command, waitErr, r.outb.String(), "")
	}

	r.mu.Lock()
	r.finished = true
	r.mu.Unlock()

	r.done <- err
	close(r.done)
	close(r.finishedCh)
}

// closeMediaPipes releases the io.Pipe ends tied to a specific run's file.
// It takes the file explicitly so a finishing run can never close pipes that
// belong to a newer run on the same transcoder.
func closeMediaPipes(file *media.File) {
	if file == nil {
		return
	}
	if file.InputPipe() && file.InputPipeReader() != nil {
		file.InputPipeReader().Close()
	}
	if file.OutputPipe() && file.OutputPipeWriter() != nil {
		file.OutputPipeWriter().Close()
	}
}
