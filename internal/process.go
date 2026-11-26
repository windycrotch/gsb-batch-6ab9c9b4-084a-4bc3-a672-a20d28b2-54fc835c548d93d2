package internal

import (
	"context"
	"os"
	"os/exec"
	"time"
)

// processWaitDelay bounds how long a canceled command may keep its I/O pipes
// open. A child or grandchild that inherited the pipes can otherwise make Wait
// block indefinitely even after the direct process was terminated.
const processWaitDelay = 2 * time.Second

// CommandContext builds an exec.Cmd whose process tree is terminated as soon
// as ctx is canceled, and whose Wait cannot hang on inherited descriptors.
func CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = processWaitDelay
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		return terminateProcess(cmd.Process)
	}
	configureProcess(cmd)
	return cmd
}
