package hls

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// DefaultMasterName is the file name of the produced master playlist. It
// matches the historical GeneratePlaylist default.
const DefaultMasterName = "playlist.m3u8"

// RenditionResult describes one successfully published resolution.
type RenditionResult struct {
	// Resolution is the preset name, e.g. "720p".
	Resolution string
	// URL is the master-playlist-relative media playlist URL, e.g. "720p.m3u8".
	URL string
	// PlaylistPath is the absolute/relative path of the published media playlist.
	PlaylistPath string
	// SegmentPaths are the published segment files for this rendition.
	SegmentPaths []string
}

// MultiResult is the outcome of an atomic multi-rendition transcode.
type MultiResult struct {
	// MasterPlaylistPath is the path of the published master playlist.
	MasterPlaylistPath string
	// MasterPlaylistURL is the relative URL of the master playlist.
	MasterPlaylistURL string
	// MasterPlaylist is the exact textual content of the master playlist.
	MasterPlaylist string
	// Resolutions are the de-duplicated resolutions in caller order.
	Resolutions []string
	// Renditions mirrors Resolutions with published file details.
	Renditions []RenditionResult

	// Compatibility aliases for the master playlist location/content. They
	// carry the same values as the canonical fields above.
	MasterPath   string
	PlaylistPath string
	MasterURL    string
	URL          string
	Path         string
	Playlist     string
}

// MultiOptions parameterizes TranscodeMultiWithOptions.
//
// Ctx may be cancelled (or carry a deadline) to abort the run; FFmpegPath is
// the injectable ffmpeg executable (a local fake script/binary works);
// MasterName defaults to playlist.m3u8.
type MultiOptions struct {
	Ctx         context.Context
	FFmpegPath  string
	SourcePath  string
	TargetDir   string
	Resolutions []string
	MasterName  string
}

// RenditionError reports which resolution failed. Its message always contains
// the resolution name and Unwrap exposes the underlying start/exit/cancel
// error (so errors.Is(err, context.Canceled) keeps working).
type RenditionError struct {
	Resolution string
	Err        error
}

func (e *RenditionError) Error() string {
	return fmt.Sprintf("hls: rendition %q failed: %v", e.Resolution, e.Err)
}

func (e *RenditionError) Unwrap() error { return e.Err }

func renditionError(res string, err error) error {
	if err == nil {
		return nil
	}
	return &RenditionError{Resolution: res, Err: err}
}

// GenerateHLSMultiWithResult behaves like GenerateHLSMulti but also returns the
// published master playlist location/content and per-rendition file paths.
func GenerateHLSMultiWithResult(ffmpegPath, srcPath, targetPath string, resolutions []string) (*MultiResult, error) {
	return TranscodeMulti(context.Background(), ffmpegPath, srcPath, targetPath, resolutions)
}

// GenerateHLSMultiWithContextResult is the context-aware result-returning form.
func GenerateHLSMultiWithContextResult(ctx context.Context, ffmpegPath, srcPath, targetPath string, resolutions []string) (*MultiResult, error) {
	return TranscodeMulti(ctx, ffmpegPath, srcPath, targetPath, resolutions)
}

// TranscodeMulti is the canonical context-aware, result-returning entry point.
func TranscodeMulti(ctx context.Context, ffmpegPath, srcPath, targetDir string, resolutions []string) (*MultiResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return runMulti(MultiOptions{
		Ctx:         ctx,
		FFmpegPath:  ffmpegPath,
		SourcePath:  srcPath,
		TargetDir:   targetDir,
		Resolutions: resolutions,
	})
}

// TranscodeMultiWithOptions runs the atomic multi-rendition transcode with an
// options struct.
func TranscodeMultiWithOptions(opts MultiOptions) (*MultiResult, error) {
	return runMulti(opts)
}

// dedupeResolutions trims whitespace, drops empty entries and de-duplicates
// presets on first occurrence while preserving order.
func dedupeResolutions(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, r := range in {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	return out
}

// checkTargetDir prepares and probes the target directory before any work.
func checkTargetDir(targetDir string) error {
	if strings.TrimSpace(targetDir) == "" {
		return fmt.Errorf("hls: target directory is empty")
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("hls: target directory %q is not usable: %w", targetDir, err)
	}
	probe, err := os.CreateTemp(targetDir, ".writeprobe-*")
	if err != nil {
		return fmt.Errorf("hls: target directory %q is not writable: %w", targetDir, err)
	}
	name := probe.Name()
	_ = probe.Close()
	_ = os.Remove(name)
	return nil
}

// publishedFile tracks a renamed-into-place artifact so a failed commit can
// roll back only files that are still ours.
type publishedFile struct {
	path string
	info os.FileInfo
}

func runMulti(opts MultiOptions) (result *MultiResult, retErr error) {
	ctx := opts.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ordered := dedupeResolutions(opts.Resolutions)
	if len(ordered) == 0 {
		return nil, fmt.Errorf("hls: at least one resolution is required")
	}
	for _, res := range ordered {
		if _, err := getConfig(res); err != nil {
			return nil, fmt.Errorf("hls: invalid resolution %q: %w", res, err)
		}
	}
	if strings.TrimSpace(opts.FFmpegPath) == "" {
		return nil, fmt.Errorf("hls: ffmpeg path is empty")
	}
	if err := checkTargetDir(opts.TargetDir); err != nil {
		return nil, err
	}

	masterName := opts.MasterName
	if strings.TrimSpace(masterName) == "" {
		masterName = DefaultMasterName
	}

	// Private staging directory inside the target tree (same volume, so the
	// final publication is a rename). Unique per invocation: concurrent
	// callers and failed-call cleanups can never touch each other's files.
	stage, err := os.MkdirTemp(opts.TargetDir, ".hls-staging-*")
	if err != nil {
		return nil, fmt.Errorf("hls: cannot create staging directory in %q: %w", opts.TargetDir, err)
	}
	stageCleaned := false
	cleanupStage := func() {
		if stageCleaned {
			return
		}
		stageCleaned = true
		_ = removeAllRetry(stage)
	}
	defer cleanupStage()

	// Transcode every rendition, in caller order, waiting for each process.
	for _, res := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, renditionError(res, err)
		}

		ffOpts, err := getOptions(opts.SourcePath, stage, res)
		if err != nil {
			return nil, renditionError(res, err)
		}

		cmd := exec.Command(opts.FFmpegPath, ffOpts...)
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		cmd.SysProcAttr = newSysProcAttr()

		if err := cmd.Start(); err != nil {
			return nil, renditionError(res, &ExecError{Err: err, Output: tailOutput(&output)})
		}
		if err := runStartedCmd(ctx, cmd, &output); err != nil {
			return nil, renditionError(res, err)
		}

		// Exit zero is not enough on its own: make sure the rendition really
		// produced something under its expected names before the master can
		// reference it.
		variantPlaylist := filepath.Join(stage, res+".m3u8")
		if _, statErr := os.Stat(variantPlaylist); statErr != nil {
			matches, _ := filepath.Glob(filepath.Join(stage, res+"_*"))
			if len(matches) == 0 {
				return nil, renditionError(res, fmt.Errorf("ffmpeg exited 0 but produced no %q artifacts", res))
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// All renditions exited successfully. Build the master playlist using the
	// unchanged variant/field format and caller order.
	variants := make([]*Variant, 0, len(ordered))
	for _, res := range ordered {
		c, _ := getConfig(res)
		variants = append(variants, &Variant{
			URL:        fmt.Sprintf("%s.m3u8", c.Name),
			Bandwidth:  c.Bandwidth,
			Resolution: c.Resolution,
		})
	}
	masterText := renderPlaylist(variants)

	masterStagePath := filepath.Join(stage, masterName)
	if err := os.WriteFile(masterStagePath, []byte(masterText), 0o644); err != nil {
		return nil, fmt.Errorf("hls: cannot stage master playlist: %w", err)
	}

	// Publish: rendition artifacts first, master playlist last (the commit).
	entries, err := os.ReadDir(stage)
	if err != nil {
		return nil, fmt.Errorf("hls: cannot read staging directory: %w", err)
	}

	published := make([]publishedFile, 0, len(entries))
	rollback := func() {
		for _, pf := range published {
			// Remove only if the directory entry still points at the file we
			// published; a concurrent caller that replaced it owns it now.
			if cur, err := os.Stat(pf.path); err == nil && os.SameFile(pf.info, cur) {
				_ = os.Remove(pf.path)
			}
		}
	}

	// First pass: everything except the master playlist name.
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == masterName {
			continue
		}
		src := filepath.Join(stage, name)
		dst := filepath.Join(opts.TargetDir, name)
		if err := os.Rename(src, dst); err != nil {
			rollback()
			return nil, fmt.Errorf("hls: cannot publish %q: %w", name, err)
		}
		if info, statErr := os.Stat(dst); statErr == nil {
			published = append(published, publishedFile{path: dst, info: info})
		} else {
			published = append(published, publishedFile{path: dst})
		}
	}

	// Commit: atomically rename the master playlist into place.
	masterDst := filepath.Join(opts.TargetDir, masterName)
	if err := os.Rename(masterStagePath, masterDst); err != nil {
		rollback()
		return nil, fmt.Errorf("hls: cannot publish master playlist %q: %w", masterName, err)
	}
	if info, err := os.Stat(masterDst); err == nil {
		published = append(published, publishedFile{path: masterDst, info: info})
	}

	// Build the detailed result from what is now visible.
	resOut := &MultiResult{
		MasterPlaylistPath: masterDst,
		MasterPlaylistURL:  masterName,
		MasterPlaylist:     masterText,
		Resolutions:        append([]string(nil), ordered...),
		MasterPath:         masterDst,
		PlaylistPath:       masterDst,
		MasterURL:          masterName,
		URL:                masterName,
		Path:               masterDst,
		Playlist:           masterText,
	}

	for _, res := range ordered {
		rr := RenditionResult{
			Resolution:   res,
			URL:          res + ".m3u8",
			PlaylistPath: filepath.Join(opts.TargetDir, res+".m3u8"),
		}
		if matches, err := filepath.Glob(filepath.Join(opts.TargetDir, res+"_*.ts")); err == nil {
			rr.SegmentPaths = matches
		}
		resOut.Renditions = append(resOut.Renditions, rr)
	}

	// Staging is now empty of everything we published; remove the directory
	// itself rather than leaving a dot-temp dir behind.
	cleanupStage()

	return resOut, nil
}
