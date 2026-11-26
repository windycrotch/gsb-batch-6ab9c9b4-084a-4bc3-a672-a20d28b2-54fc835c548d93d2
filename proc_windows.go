//go:build windows
// +build windows

package hls

import (
	"os/exec"
	"strconv"
	"syscall"
)

// createNewProcessGroup is the Win32 CREATE_NEW_PROCESS_GROUP flag.
const createNewProcessGroup = 0x00000200

func newSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: createNewProcessGroup}
}

// killProcessTree force-kills the child together with every process it spawned
// via taskkill /T, falling back to killing just the child. This matters for
// fake ffmpeg entry points implemented as .bat/.cmd wrappers.
func killProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	_ = exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid)).Run()
	_ = cmd.Process.Kill()
}
