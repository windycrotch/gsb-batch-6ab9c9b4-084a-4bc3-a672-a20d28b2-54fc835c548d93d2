package hls

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

// maxOutputTail bounds how much ffmpeg output is attached to an error.
const maxOutputTail = 4096

// runStartedCmd waits for a started command to finish. When ctx is cancelled
// (or its deadline fires) the whole child process tree is terminated. It always
// reaps the child and reports its real exit status; it never returns after a
// mere Start.
func runStartedCmd(ctx context.Context, cmd *exec.Cmd, output *bytes.Buffer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		killProcessTree(cmd)
		return err
	}

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- cmd.Wait()
	}()

	select {
	case err := <-waitErr:
		// Cancellation is authoritative even if both cases were ready and the
		// child happened to exit non-zero when killed: callers must still see
		// errors.Is(err, context.Canceled/DeadlineExceeded).
		if cerr := ctx.Err(); cerr != nil {
			killProcessTree(cmd)
			return fmt.Errorf("ffmpeg interrupted: %w", cerr)
		}
		if err != nil {
			return &ExecError{Err: err, Output: tailOutput(output)}
		}
		return nil
	case <-ctx.Done():
		// Cancel requested: kill the child (and any children it spawned),
		// then still reap it so no zombie/handle lingers.
		killProcessTree(cmd)
		<-waitErr
		return fmt.Errorf("ffmpeg interrupted: %w", ctx.Err())
	}
}

// ExecError wraps an ffmpeg start/wait failure together with the tail of its
// stderr/stdout output.
type ExecError struct {
	// Err is the underlying error (often an *exec.ExitError or a start error).
	Err error
	// Output is the tail of the captured combined output.
	Output string
}

func (e *ExecError) Error() string {
	if e.Output != "" {
		return fmt.Sprintf("%v\nffmpeg output (tail):\n%s", e.Err, e.Output)
	}
	return e.Err.Error()
}

func (e *ExecError) Unwrap() error { return e.Err }

func tailOutput(buf *bytes.Buffer) string {
	if buf == nil || buf.Len() == 0 {
		return ""
	}
	out := buf.String()
	if len(out) > maxOutputTail {
		out = "..." + out[len(out)-maxOutputTail:]
	}
	return out
}
