// Package hls provides a few functionalities to generate HLS
// files using ffmpeg.
package hls

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

// GenerateHLS will generate HLS file based on resolution presets.
// The available resolutions are: 360p, 480p, 720p and 1080p.
//
// It waits for the ffmpeg subprocess to finish and reports its real exit
// status: a missing ffmpeg executable or a non-zero ffmpeg exit code is
// returned as an error.
func GenerateHLS(ffmpegPath, srcPath, targetPath, resolution string) error {
	options, err := getOptions(srcPath, targetPath, resolution)
	if err != nil {
		return err
	}

	return GenerateHLSCustom(ffmpegPath, options)
}

// GenerateHLSCustom will generate HLS using the flexible options params.
// options is array of string that accepted by ffmpeg command.
//
// Unlike the original fire-and-forget implementation, this waits for ffmpeg to
// exit and returns a non-nil error when the executable cannot be started or
// ffmpeg exits non-zero.
func GenerateHLSCustom(ffmpegPath string, options []string) error {
	return GenerateHLSCustomContext(context.Background(), ffmpegPath, options)
}

// GenerateHLSCustomContext is GenerateHLSCustom with cancellation. When ctx is
// cancelled or exceeds its deadline the ffmpeg process tree is killed and the
// returned error wraps ctx.Err().
func GenerateHLSCustomContext(ctx context.Context, ffmpegPath string, options []string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	cmd := exec.Command(ffmpegPath, options...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = newSysProcAttr()

	if err := cmd.Start(); err != nil {
		return &ExecError{Err: err}
	}

	return waitWithStreams(ctx, cmd)
}

// waitWithStreams reaps a command whose output is wired directly to the
// process streams. It is separated from runStartedCmd because an in-memory
// output buffer cannot be used at the same time as os.Stdout/os.Stderr.
func waitWithStreams(ctx context.Context, cmd *exec.Cmd) error {
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
		if cerr := ctx.Err(); cerr != nil {
			killProcessTree(cmd)
			return fmt.Errorf("ffmpeg interrupted: %w", cerr)
		}
		if err != nil {
			return &ExecError{Err: err}
		}
		return nil
	case <-ctx.Done():
		killProcessTree(cmd)
		<-waitErr
		return fmt.Errorf("ffmpeg interrupted: %w", ctx.Err())
	}
}
