//go:build windows

package internal

import (
	"syscall"
	"unsafe"
)

// processEntrySize reports sizeof(PROCESSENTRY32W) so the toolhelp snapshot
// API version-checks the structure correctly.
func processEntrySize() uint32 {
	var entry syscall.ProcessEntry32
	return uint32(unsafe.Sizeof(entry))
}
