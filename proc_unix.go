//go:build !windows
// +build !windows

package hls

import (
	"os/exec"
	"syscall"
)

// newSysProcAttr puts every ffmpeg invocation into its own process group so a
// signal can be delivered to the whole tree (ffmpeg does not normally spawn
// children, but shell-injected fakes might).
func newSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// killProcessTree kills the process group of cmd.
func killProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	// A negative pid sends the signal to the whole process group, whose
	// leader is the child because Setpgid was set.
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = cmd.Process.Kill()
}
