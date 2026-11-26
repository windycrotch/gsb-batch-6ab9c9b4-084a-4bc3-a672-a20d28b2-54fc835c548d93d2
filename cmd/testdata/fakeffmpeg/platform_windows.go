//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

func execCommand(name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	// CREATE_NO_WINDOW keeps the detached worker from flashing a console.
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x08000000}
	return cmd
}

// setOrphan leaves the worker running after the parent exits.
func setOrphan(cmd *exec.Cmd) {}
