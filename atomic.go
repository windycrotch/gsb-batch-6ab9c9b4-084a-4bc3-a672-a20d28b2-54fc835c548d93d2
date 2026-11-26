package hls

import (
	"os"
	"path/filepath"
	"time"
)

// atomicWriteFile writes data to a temp file in the destination directory,
// fsyncs it and renames it over destination. The rename is the commit point:
// another reader sees either the old complete file or the new complete file.
func atomicWriteFile(destination string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(destination)

	tmp, err := os.CreateTemp(dir, ".hls-master-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Remove the temp file if we fail before the rename commits.
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil && !os.IsNotExist(err) {
		// Chmod is best-effort on platforms with limited permission support.
		if !os.IsPermission(err) {
			return err
		}
	}
	if err := os.Rename(tmpName, destination); err != nil {
		return err
	}
	committed = true
	return nil
}

// removeAllRetry removes a path, retrying briefly on Windows where a just
// killed process can hold file handles for a few milliseconds.
func removeAllRetry(path string) error {
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		err = os.RemoveAll(path)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
	}
	return err
}
