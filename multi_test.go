package hls

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeFFmpegSource is a portable fake ffmpeg: it understands just enough of
// getOptions' arguments to materialize the variant playlist and segments, and
// can be told (via env) to fail for a resolution or sleep forever. Because it
// is a real compiled binary, start/wait/exit/cancel/tree-kill semantics are
// exercised exactly as with ffmpeg.
const fakeFFmpegSource = `package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	args := os.Args[1:]
	segPattern := ""
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-hls_segment_filename" {
			segPattern = args[i+1]
		}
	}
	outM3U8 := ""
	if len(args) > 0 {
		outM3U8 = args[len(args)-1]
	}
	res := strings.TrimSuffix(filepath.Base(outM3U8), filepath.Ext(outM3U8))

	sleepRes := os.Getenv("FAKE_SLEEP_RES")
	sleepAll := os.Getenv("FAKE_SLEEP_ALL")
	if (sleepRes != "" && sleepRes == res) || sleepAll == "1" {
		ms := 30000
		if v := os.Getenv("FAKE_SLEEP_MS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				ms = n
			}
		}
		if sf := os.Getenv("FAKE_STARTED_FILE"); sf != "" {
			_ = os.WriteFile(sf+".started-"+res, []byte("x"), 0644)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	if outM3U8 == "" {
		fmt.Fprintln(os.Stderr, "fake: no output playlist argument")
		os.Exit(2)
	}
	dir := filepath.Dir(outM3U8)
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "fake mkdir:", err)
		os.Exit(2)
	}

	marker := os.Getenv("FAKE_MARKER")
	body := "fake-segment marker=" + marker + " pattern=" + segPattern + "\n"
	made := []string{}
	if segPattern != "" {
		if strings.Contains(segPattern, "%") {
			for i := 0; i < 2; i++ {
				name := strings.Replace(segPattern, "%03d", fmt.Sprintf("%03d", i), 1)
				if err := os.WriteFile(name, []byte(body), 0644); err != nil {
					fmt.Fprintln(os.Stderr, "fake write seg:", err)
					os.Exit(2)
				}
				made = append(made, filepath.Base(name))
			}
		} else {
			if err := os.WriteFile(segPattern, []byte(body), 0644); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			made = append(made, filepath.Base(segPattern))
		}
	}

	pl := "#EXTM3U\n#EXT-X-VERSION:3\n"
	for _, s := range made {
		pl += "#EXTINF:10.0,\n" + s + "\n"
	}
	if err := os.WriteFile(outM3U8, []byte(pl), 0644); err != nil {
		fmt.Fprintln(os.Stderr, "fake write playlist:", err)
		os.Exit(2)
	}

	if fr := os.Getenv("FAKE_FAIL_RES"); fr != "" {
		for _, r := range strings.Split(fr, ",") {
			r = strings.TrimSpace(r)
			if r == res || r == "*" {
				fmt.Fprintf(os.Stderr, "fake ffmpeg forced failure for %s\n", res)
				os.Exit(1)
			}
		}
	}
	os.Exit(0)
}
`

var (
	fakeBuildOnce sync.Once
	fakeBuildPath string
	fakeBuildErr  error
)

func buildFakeFFmpeg(t *testing.T) string {
	t.Helper()
	fakeBuildOnce.Do(func() {
		binDir, err := os.MkdirTemp("", "hls-fake-bin-")
		if err != nil {
			fakeBuildErr = err
			return
		}
		srcDir := filepath.Join(binDir, "src")
		if err := os.MkdirAll(srcDir, 0o755); err != nil {
			fakeBuildErr = err
			return
		}
		if err := os.WriteFile(filepath.Join(srcDir, "go.mod"), []byte("module fakeff\n\ngo 1.24\n"), 0o644); err != nil {
			fakeBuildErr = err
			return
		}
		if err := os.WriteFile(filepath.Join(srcDir, "main.go"), []byte(fakeFFmpegSource), 0o644); err != nil {
			fakeBuildErr = err
			return
		}
		exeName := "ffmpeg-fake"
		if filepath.Ext(os.Args[0]) == ".exe" || strings.Contains(strings.ToLower(os.Getenv("OS")), "windows") {
			exeName += ".exe"
		}
		out := filepath.Join(binDir, exeName)
		cmd := exec.Command("go", "build", "-o", out, ".")
		cmd.Dir = srcDir
		cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local", "CGO_ENABLED=0")
		if combined, err := cmd.CombinedOutput(); err != nil {
			fakeBuildErr = fmt.Errorf("build fake ffmpeg: %w\n%s", err, combined)
			return
		}
		fakeBuildPath = out
	})
	if fakeBuildErr != nil {
		t.Fatalf("fake ffmpeg unavailable: %v", fakeBuildErr)
	}
	return fakeBuildPath
}

// masterURLs returns the non-comment lines of a master playlist.
func masterURLs(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read master playlist: %v", err)
	}
	var urls []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		urls = append(urls, line)
	}
	return urls
}

func countStreamInf(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read master playlist: %v", err)
	}
	return strings.Count(string(data), "#EXT-X-STREAM-INF:")
}

func assertNoStagingDirs(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".hls-staging-") {
			t.Fatalf("staging directory left behind: %s", e.Name())
		}
		if strings.HasPrefix(e.Name(), ".hls-master-") || strings.HasPrefix(e.Name(), ".writeprobe-") {
			t.Fatalf("temporary file left behind: %s", e.Name())
		}
	}
}

func TestMultiSuccessOrderAndFiles(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	dir := t.TempDir()

	res, err := TranscodeMulti(context.Background(), ff, filepath.Join(dir, "in.mp4"), dir, []string{"360p", "720p"})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if got, want := res.Resolutions, []string{"360p", "720p"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("resolutions = %v, want %v", got, want)
	}

	master := filepath.Join(dir, "playlist.m3u8")
	urls := masterURLs(t, master)
	if got, want := strings.Join(urls, ","), "360p.m3u8,720p.m3u8"; got != want {
		t.Fatalf("master urls = %v, want [%s]", urls, want)
	}
	if n := countStreamInf(t, master); n != 2 {
		t.Fatalf("STREAM-INF count = %d, want 2", n)
	}
	if !strings.Contains(res.MasterPlaylist, "BANDWIDTH=800000") ||
		!strings.Contains(res.MasterPlaylist, "RESOLUTION=640x360") ||
		!strings.Contains(res.MasterPlaylist, "BANDWIDTH=2800000") {
		t.Fatalf("master missing preset fields:\n%s", res.MasterPlaylist)
	}

	for _, f := range []string{"360p.m3u8", "720p.m3u8", "360p_000.ts", "360p_001.ts", "720p_000.ts", "720p_001.ts"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected artifact %s: %v", f, err)
		}
	}
	assertNoStagingDirs(t, dir)
}

func TestMultiFailureNamesResolutionAndHidesArtifacts(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	t.Setenv("FAKE_FAIL_RES", "720p")
	dir := t.TempDir()

	// Exercise the flexible flagship entry with the repository-style slice.
	err := GenerateHLSMulti(ff, filepath.Join(dir, "in.mp4"), dir, []string{"360p", "720p"})
	if err == nil {
		t.Fatal("expected error for failing 720p, got nil")
	}
	if !strings.Contains(err.Error(), "720p") {
		t.Fatalf("error %q does not name failed resolution 720p", err.Error())
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("failed run must not leave anything visible, found %q", e.Name())
	}
}

func TestMultiMissingFFmpegReportsError(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "no-such-ffmpeg-binary")

	err := GenerateHLSMulti(missing, "in.mp4", dir, []string{"360p"})
	if err == nil {
		t.Fatal("missing ffmpeg path must return an error")
	}
	assertNoStagingDirs(t, dir)
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".hls-staging-") {
			t.Fatalf("staging leaked after start failure: %s", e.Name())
		}
	}
}

func TestMultiUnwritableTargetReportsError(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	base := t.TempDir()

	// Cross-platform "not a writable directory": target path traverses a
	// regular file, so MkdirAll/temp creation must fail.
	blocker := filepath.Join(base, "regular-file")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(blocker, "out")

	if _, err := TranscodeMulti(context.Background(), ff, "in.mp4", target, []string{"360p"}); err == nil {
		t.Fatal("unwritable/invalid target directory must return an error")
	}

	// Best-effort read-only directory check (meaningful on Unix; on Windows
	// the read-only attribute does not deny creating files, so skip there if
	// the probe still succeeds).
	ro := filepath.Join(base, "readonly")
	if err := os.MkdirAll(ro, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ro, 0o555); err != nil {
		t.Skipf("chmod unsupported: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(ro, 0o755) })
	if f, err := os.CreateTemp(ro, "probe-*"); err == nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		t.Log("platform does not enforce read-only directories; skipping denial assertion")
		return
	}
	if err := GenerateHLSMulti(ff, "in.mp4", ro, []string{"360p"}); err == nil {
		t.Fatal("read-only target directory must return an error")
	}
}

func TestMultiDeduplicatesAndPreservesOrder(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	dir := t.TempDir()

	err := GenerateHLSMulti(ff, "in.mp4", dir, []string{"480p", "360p", "480p"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	urls := masterURLs(t, filepath.Join(dir, "playlist.m3u8"))
	if got, want := strings.Join(urls, ","), "480p.m3u8,360p.m3u8"; got != want {
		t.Fatalf("urls = %v, want [%s] (dedupe first-occurrence order)", urls, want)
	}
	if n := countStreamInf(t, filepath.Join(dir, "playlist.m3u8")); n != 2 {
		t.Fatalf("STREAM-INF count = %d, want exactly 2", n)
	}
}

func TestMultiInvalidResolution(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	dir := t.TempDir()
	if err := GenerateHLSMulti(ff, "in.mp4", dir, []string{"480p", "9999p"}); err == nil {
		t.Fatal("unknown preset must error")
	} else if !strings.Contains(err.Error(), "9999p") {
		t.Fatalf("error should name invalid preset, got %v", err)
	}
}

func TestMultiConcurrencyOneFails(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	t.Setenv("FAKE_FAIL_RES", "1080p")
	dir := t.TempDir()

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		errs[0] = GenerateHLSMulti(ff, "in-a.mp4", dir, []string{"360p", "720p"})
	}()
	go func() {
		defer wg.Done()
		<-start
		errs[1] = GenerateHLSMulti(ff, "in-b.mp4", dir, []string{"480p", "1080p"})
	}()
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent transcodes timed out")
	}

	if errs[0] != nil {
		t.Fatalf("expected caller A success, got %v", errs[0])
	}
	if errs[1] == nil || !strings.Contains(errs[1].Error(), "1080p") {
		t.Fatalf("expected caller B error naming 1080p, got %v", errs[1])
	}

	// Winner master and every referenced artifact is present.
	master := filepath.Join(dir, "playlist.m3u8")
	urls := masterURLs(t, master)
	if got, want := strings.Join(urls, ","), "360p.m3u8,720p.m3u8"; got != want {
		t.Fatalf("winning master urls = %v, want [%s]", urls, want)
	}
	for _, u := range urls {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(u))); err != nil {
			t.Errorf("master references missing %s: %v", u, err)
		}
	}
	for _, f := range []string{"360p_000.ts", "720p_000.ts"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("winner segment %s missing: %v", f, err)
		}
	}

	// Failed caller's successful-looking earlier rendition was never published.
	for _, f := range []string{"480p.m3u8", "480p_000.ts", "1080p.m3u8"} {
		if _, err := os.Stat(filepath.Join(dir, f)); !os.IsNotExist(err) {
			t.Errorf("failed caller artifact %s must not be visible (err=%v)", f, err)
		}
	}
	assertNoStagingDirs(t, dir)
}

func TestMultiConcurrencyBothSucceed(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	dir := t.TempDir()

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]*MultiResult, 2)
	errs := make([]error, 2)
	sets := [][]string{{"360p", "480p"}, {"720p", "1080p"}}
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = TranscodeMulti(context.Background(), ff, fmt.Sprintf("in-%d.mp4", i), dir, sets[i])
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent transcodes timed out")
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d unexpected error: %v", i, err)
		}
	}

	// Each returned result stays consistent with its own resolution set.
	for i, r := range results {
		if strings.Join(r.Resolutions, ",") != strings.Join(sets[i], ",") {
			t.Fatalf("result %d resolutions = %v", i, r.Resolutions)
		}
		for _, res := range sets[i] {
			if !strings.Contains(r.MasterPlaylist, res+".m3u8") {
				t.Errorf("result %d master missing %s", i, res)
			}
		}
	}

	// All disjoint rendition artifacts from both callers coexist.
	wantFiles := []string{
		"360p.m3u8", "480p.m3u8", "720p.m3u8", "1080p.m3u8",
		"360p_000.ts", "480p_000.ts", "720p_000.ts", "1080p_000.ts",
	}
	sort.Strings(wantFiles)
	for _, f := range wantFiles {
		data, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Errorf("shared dir missing %s: %v", f, err)
			continue
		}
		if !strings.Contains(string(data), "fake-segment") && strings.HasSuffix(f, ".ts") {
			t.Errorf("segment %s content not from fake run", f)
		}
	}

	// Whatever master playlist currently wins must be fully resolvable.
	for _, u := range masterURLs(t, filepath.Join(dir, "playlist.m3u8")) {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(u))); err != nil {
			t.Errorf("on-disk master references missing %s: %v", u, err)
		}
	}
	assertNoStagingDirs(t, dir)
}

func TestMultiCancelKillsCleansAndReruns(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	base := t.TempDir()
	startedMarker := filepath.Join(base, "started")
	t.Setenv("FAKE_SLEEP_RES", "360p")
	t.Setenv("FAKE_SLEEP_MS", "30000")
	t.Setenv("FAKE_STARTED_FILE", startedMarker)

	dir := filepath.Join(base, "out")
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- GenerateHLSMultiContext(ctx, ff, "in.mp4", dir, []string{"360p", "720p"})
	}()

	// Wait until the fake ffmpeg is actually sleeping.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(startedMarker + ".started-360p"); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(startedMarker + ".started-360p"); err != nil {
		t.Fatalf("sleeping fake ffmpeg never started: %v", err)
	}

	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error %v does not wrap context.Canceled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cancelled transcode did not return; child process likely not killed")
	}

	// No published/staging residue.
	if entries, err := os.ReadDir(dir); err == nil {
		for _, e := range entries {
			t.Errorf("post-cancel residue: %s", e.Name())
		}
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat dir after cancel: %v", err)
	}

	// Directory is usable again: use a resolution the sleep rule ignores.
	res2, err := TranscodeMulti(context.Background(), ff, "in.mp4", dir, []string{"720p"})
	if err != nil {
		t.Fatalf("re-run after cancel failed: %v", err)
	}
	if _, err := os.Stat(res2.MasterPlaylistPath); err != nil {
		t.Fatalf("re-run master missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "720p_000.ts")); err != nil {
		t.Fatalf("re-run segment missing: %v", err)
	}
	assertNoStagingDirs(t, dir)
}

func TestGenerateHLSCustomWaitsAndReportsStatus(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	dir := t.TempDir()

	// Success path really waits (a sleeping fake proves we don't Start-and-return).
	t.Setenv("FAKE_SLEEP_ALL", "1")
	t.Setenv("FAKE_SLEEP_MS", "400")
	start := time.Now()
	if err := GenerateHLSCustom(ff, []string{"-i", "in.mp4", filepath.Join(dir, "ok.m3u8")}); err != nil {
		t.Fatalf("custom run failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("GenerateHLSCustom returned after %v; expected it to wait for exit", elapsed)
	}

	// Non-zero exit is reported.
	t.Setenv("FAKE_SLEEP_ALL", "")
	t.Setenv("FAKE_FAIL_RES", "*")
	if err := GenerateHLSCustom(ff, []string{"-i", "in.mp4", filepath.Join(dir, "fail.m3u8")}); err == nil {
		t.Fatal("non-zero ffmpeg exit must be reported by GenerateHLSCustom")
	}

	// Missing executable is reported.
	if err := GenerateHLSCustom(filepath.Join(dir, "missing-ff"), nil); err == nil {
		t.Fatal("missing executable must be reported by GenerateHLSCustom")
	}
}

func TestFlexibleArgumentShapes(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	dir := t.TempDir()

	// context first.
	if err := GenerateHLSMulti(context.Background(), ff, "in.mp4", filepath.Join(dir, "a"), []string{"360p"}); err != nil {
		t.Fatalf("ctx-first shape: %v", err)
	}
	// varargs resolutions.
	if err := GenerateHLSMulti(ff, "in.mp4", filepath.Join(dir, "b"), "480p", "360p"); err != nil {
		t.Fatalf("varargs shape: %v", err)
	}
	// options struct.
	if err := GenerateHLSMulti(MultiOptions{
		FFmpegPath:  ff,
		SourcePath:  "in.mp4",
		TargetDir:   filepath.Join(dir, "c"),
		Resolutions: []string{"720p"},
	}); err != nil {
		t.Fatalf("options struct shape: %v", err)
	}
	// typed options entry.
	if _, err := TranscodeMultiWithOptions(MultiOptions{
		FFmpegPath:  ff,
		SourcePath:  "in.mp4",
		TargetDir:   filepath.Join(dir, "d"),
		Resolutions: []string{"1080p"},
	}); err != nil {
		t.Fatalf("TranscodeMultiWithOptions: %v", err)
	}

	// Named string/slice types must parse too.
	type namedRes string
	type namedResList []namedRes
	if err := GenerateHLSMulti(ff, "in.mp4", filepath.Join(dir, "e"), namedResList{"360p", "480p"}); err != nil {
		t.Fatalf("named slice shape: %v", err)
	}
	if err := GenerateHLSMulti(namedRes(ff), namedRes("in.mp4"), filepath.Join(dir, "f"), []string{"720p"}); err != nil {
		t.Fatalf("named string shape: %v", err)
	}
}

func TestRenderPlaylistFormatPinned(t *testing.T) {
	got := renderPlaylist([]*Variant{{
		URL:        "720p.m3u8",
		Bandwidth:  "2800000",
		Resolution: "1280x720",
	}})
	want := "#EXTM3U\n" +
		"#EXT-X-VERSION:3\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=2800000,RESOLUTION=1280x720\n" +
		"720p.m3u8\n"
	if got != want {
		t.Fatalf("playlist format changed:\ngot:\n%q\nwant:\n%q", got, want)
	}
}

func TestMultiMasterPublishFailureReportedAndRollsBack(t *testing.T) {
	ff := buildFakeFFmpeg(t)
	dir := t.TempDir()

	// Block the atomic master rename by occupying playlist.m3u8 with a
	// directory. The variant artifacts publish first; the failed commit must
	// return an error and roll them back, leaving no half master file.
	blocker := filepath.Join(dir, "playlist.m3u8")
	if err := os.Mkdir(blocker, 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := TranscodeMulti(context.Background(), ff, "in.mp4", dir, []string{"360p", "720p"})
	if err == nil {
		t.Fatal("expected master publication error")
	}

	// The blocker is a directory, not a half-written playlist file.
	if info, statErr := os.Stat(blocker); statErr != nil || !info.IsDir() {
		t.Fatalf("expected blocker dir intact, got info=%v err=%v", info, statErr)
	}

	// Rollback removes the published rendition files of this failed commit.
	for _, f := range []string{"360p.m3u8", "720p.m3u8", "360p_000.ts", "720p_000.ts"} {
		if _, err := os.Stat(filepath.Join(dir, f)); !os.IsNotExist(err) {
			t.Errorf("published artifact %s should have been rolled back (err=%v)", f, err)
		}
	}
	assertNoStagingDirs(t, dir)

	// The error from the playlist writer itself is not swallowed either.
	if err := GeneratePlaylistE([]*Variant{{URL: "360p.m3u8", Bandwidth: "800000"}}, dir, filepath.Join("no-such-subdir", "p.m3u8")); err == nil {
		t.Fatal("GeneratePlaylistE with a bad parent must return an error")
	}
}
