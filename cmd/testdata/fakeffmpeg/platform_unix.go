//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

func execCommand(name string, args ...string) *exec.Cmd {
	return exec.Command(name, args...)
}

// setOrphan detaches the worker so it survives this process exiting.
func setOrphan(cmd *exec.Cmd) {
	_ = syscall.Setsid
}
