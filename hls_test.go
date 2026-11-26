package hls

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"
)

func TestCmdExecuteFfmpeg(t *testing.T) {
	base, _ := os.Getwd()

	targetPath := path.Join(base, "static")
	srcPath := path.Join(base, "static", "sample.mov")
	ffmpegPath := "/usr/local/bin/ffmpeg"

	// The historical test targets a developer machine with a fixed ffmpeg
	// install and a committed sample clip. On other machines (CI, Windows,
	// fake-script verification) skip rather than panic; the wait/exit-status
	// behaviour is covered hermetically in multi_test.go.
	if _, err := os.Stat(srcPath); err != nil {
		t.Skipf("sample source %s not available: %v", srcPath, err)
	}
	if _, err := os.Stat(ffmpegPath); err != nil {
		resolved, lookErr := exec.LookPath("ffmpeg")
		if lookErr != nil {
			t.Skipf("ffmpeg not available at %s or on PATH", ffmpegPath)
		}
		ffmpegPath = resolved
	}
	if err := os.MkdirAll(filepath.FromSlash(targetPath), 0o755); err != nil {
		t.Fatalf("cannot create target dir: %v", err)
	}

	err := GenerateHLS(ffmpegPath, srcPath, targetPath, "480p")
	if err != nil {
		panic(err)
	}
}
