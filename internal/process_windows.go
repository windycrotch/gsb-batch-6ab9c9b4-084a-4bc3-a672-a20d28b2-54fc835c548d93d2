//go:build windows

package internal

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

var (
	kernel32             = syscall.NewLazyDLL("kernel32.dll")
	procOpenProcess      = kernel32.NewProc("OpenProcess")
	procTerminateProcess = kernel32.NewProc("TerminateProcess")
)

const (
	processTerminateAccess = 0x0001
	processQueryAccess     = 0x1000
)

func configureProcess(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP
}

// terminateProcess stops the direct process and every descendant it spawned.
// Windows has no process-group signal, so the tree is enumerated through
// toolhelp snapshots; terminating only the direct PID would leave background
// workers, which inherit stdout pipes, running after the request ends.
func terminateProcess(process *os.Process) error {
	if process == nil {
		return os.ErrProcessDone
	}
	if err := terminateTree(uint32(process.Pid)); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return os.ErrProcessDone
		}
		// Fall back to the direct process if the tree walk itself failed.
		if killErr := process.Kill(); killErr != nil {
			if errors.Is(killErr, os.ErrProcessDone) {
				return os.ErrProcessDone
			}
			return killErr
		}
	}
	return nil
}

// terminateTree kills pid and its descendants, children first, so a parent
// cannot respawn workers while the walk is still running.
func terminateTree(pid uint32) error {
	for _, child := range childProcesses(pid) {
		_ = terminateTree(child)
	}
	handle, err := openProcess(processTerminateAccess|processQueryAccess, pid)
	if err != nil {
		if isNotExist(err) {
			return os.ErrProcessDone
		}
		return err
	}
	defer syscall.CloseHandle(handle)

	r1, _, callErr := procTerminateProcess.Call(uintptr(handle), 1)
	if r1 == 0 {
		if isNotExist(callErr) {
			return os.ErrProcessDone
		}
		return fmt.Errorf("terminate process %d: %w", pid, callErr)
	}
	return nil
}

func openProcess(access uint32, pid uint32) (syscall.Handle, error) {
	r1, _, err := procOpenProcess.Call(uintptr(access), 0, uintptr(pid))
	if r1 == 0 {
		return 0, err
	}
	return syscall.Handle(r1), nil
}

func isNotExist(err error) bool {
	// ERROR_INVALID_PARAMETER (87) is returned when the PID already exited.
	var errno syscall.Errno
	return errors.As(err, &errno) && (errno == syscall.ESRCH || errno == 87)
}

// childProcesses returns the direct children of pid from a toolhelp snapshot.
func childProcesses(pid uint32) []uint32 {
	snapshot, err := syscall.CreateToolhelp32Snapshot(syscall.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return nil
	}
	defer syscall.CloseHandle(snapshot)

	var entry syscall.ProcessEntry32
	entry.Size = processEntrySize()
	if err := syscall.Process32First(snapshot, &entry); err != nil {
		return nil
	}
	var children []uint32
	for {
		if entry.ParentProcessID == pid && entry.ProcessID != pid {
			children = append(children, entry.ProcessID)
		}
		if err := syscall.Process32Next(snapshot, &entry); err != nil {
			break
		}
	}
	return children
}
