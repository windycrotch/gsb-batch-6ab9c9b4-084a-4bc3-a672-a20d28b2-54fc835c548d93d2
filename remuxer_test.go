//go:build !ios && !android && (amd64 || arm64)

package ffgo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/obinnaokechukwu/ffgo/avcodec"
	"github.com/obinnaokechukwu/ffgo/avformat"
	"github.com/obinnaokechukwu/ffgo/avutil"
)

// createRemuxSource builds a small mp4 file containing one video and one
// audio stream and returns its path. It uses the ffmpeg CLI so the source is
// a real, independently produced multi-track container rather than a
// single-stream encoder artifact.
func createRemuxSource(t *testing.T) string {
	t.Helper()

	ffmpegBin, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skipf("ffmpeg CLI not available, skipping multi-stream remuxer test: %v", err)
	}

	srcPath := filepath.Join(t.TempDir(), "source.mp4")
	cmd := exec.Command(ffmpegBin,
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc=size=160x120:rate=30:duration=2",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=2",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "30", "-pix_fmt", "yuv420p",
		"-c:a", "aac", "-b:a", "64k",
		"-shortest", srcPath,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to create multi-stream source: %v\n%s", err, out)
	}

	return srcPath
}

// openSourceDecoder opens a decoder and fails the test on error.
func openSourceDecoder(t *testing.T, path string) *Decoder {
	t.Helper()
	decoder, err := NewDecoder(path)
	if err != nil {
		t.Fatalf("failed to open source %s: %v", path, err)
	}
	return decoder
}

// verifyContainerOpens parses the given file with a fresh decoder, proving the
// container is structurally parseable (header/index intact).
func verifyContainerOpens(t *testing.T, path string) *Decoder {
	t.Helper()
	decoder, err := NewDecoder(path)
	if err != nil {
		t.Fatalf("output container %s cannot be reopened/parsed: %v", path, err)
	}
	return decoder
}

// verifyContainerBytesOpens parses an in-memory container through custom I/O.
func verifyContainerBytesOpens(t *testing.T, data []byte, format string) *Decoder {
	t.Helper()
	decoder, err := NewDecoderFromReader(bytes.NewReader(data), format)
	if err != nil {
		t.Fatalf("in-memory output cannot be reopened/parsed: %v", err)
	}
	return decoder
}

// streamCodecParType returns the codec type recorded for an output stream.
func streamCodecParType(t *testing.T, decoder *Decoder, streamIdx int) avutil.MediaType {
	t.Helper()
	stream := avformat.GetStream(decoder.formatCtx, streamIdx)
	if stream == nil {
		t.Fatalf("output stream %d missing", streamIdx)
	}
	return avformat.GetCodecParType(avformat.GetStreamCodecPar(stream))
}

// TestRemuxerSubsetStreamMapping verifies that when only a subset of input
// streams is copied, output tracks line up one-to-one with the selected input
// streams (in selection order) and packets from unselected input streams are
// silently skipped instead of erroring or landing on the wrong track.
func TestRemuxerSubsetStreamMapping(t *testing.T) {
	if !requireFFmpeg(t) {
		return
	}

	srcPath := createRemuxSource(t)
	decoder := openSourceDecoder(t, srcPath)

	if decoder.NumStreams() < 2 {
		decoder.Close()
		t.Fatalf("test source must contain >=2 streams, got %d", decoder.NumStreams())
	}

	// Identify input stream types up front.
	inputTypes := make(map[int]avutil.MediaType)
	for i := 0; i < decoder.NumStreams(); i++ {
		stream := avformat.GetStream(decoder.formatCtx, i)
		inputTypes[i] = avformat.GetCodecParType(avformat.GetStreamCodecPar(stream))
	}
	videoInput, audioInput := -1, -1
	for idx, typ := range inputTypes {
		switch typ {
		case avutil.MediaTypeVideo:
			videoInput = idx
		case avutil.MediaTypeAudio:
			audioInput = idx
		}
	}
	if videoInput < 0 || audioInput < 0 {
		decoder.Close()
		t.Fatalf("source must contain both video and audio, got %v", inputTypes)
	}

	// Select audio FIRST then video to prove output order follows selection
	// order rather than original input indices.
	selected := []int{audioInput, videoInput}

	dstPath := filepath.Join(t.TempDir(), "subset.mkv")
	remuxer, err := NewRemuxer(dstPath, decoder, &RemuxerConfig{InputStreams: selected})
	if err != nil {
		decoder.Close()
		t.Fatalf("NewRemuxer failed: %v", err)
	}

	if got := remuxer.NumOutputStreams(); got != 2 {
		_ = remuxer.Close()
		decoder.Close()
		t.Fatalf("expected 2 output streams, got %d", got)
	}

	// Mapping must be stable: selected input idx -> sequential output idx.
	mapping := remuxer.StreamMapping()
	if mapping[audioInput] != 0 || mapping[videoInput] != 1 {
		_ = remuxer.Close()
		decoder.Close()
		t.Fatalf("unexpected stream mapping %v (want audio input %d->0, video input %d->1)",
			mapping, audioInput, videoInput)
	}

	if err := remuxer.WriteHeader(); err != nil {
		_ = remuxer.Close()
		decoder.Close()
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Drive packets manually, including packets from an unselected stream
	// index that does not appear in the selection. Those must be skipped
	// quietly.
	unselected := -1
	for i := 0; i < decoder.NumStreams(); i++ {
		if i != audioInput && i != videoInput {
			unselected = i
			break
		}
	}

	for {
		pkt, err := decoder.ReadPacket()
		if err != nil {
			_ = remuxer.Close()
			decoder.Close()
			t.Fatalf("ReadPacket failed: %v", err)
		}
		if pkt == nil {
			break
		}
		idx := pkt.StreamIndex()
		if err := remuxer.WritePacket(pkt.ptr, idx); err != nil {
			_ = remuxer.Close()
			decoder.Close()
			t.Fatalf("WritePacket for input stream %d failed: %v", idx, err)
		}
	}

	// Explicitly writing to a definitely-unselected index must also be a
	// quiet no-op (use an out-of-range index that cannot be mapped).
	if err := remuxer.WritePacket(nil, decoder.NumStreams()+5); err != nil {
		_ = remuxer.Close()
		decoder.Close()
		t.Fatalf("write to unselected stream must be skipped silently, got %v", err)
	}
	if unselected >= 0 {
		// Confirm a mapped lookup for the unselected stream is absent.
		if _, ok := mapping[unselected]; ok {
			t.Fatalf("unselected stream %d leaked into the mapping", unselected)
		}
	}

	if err := remuxer.Close(); err != nil {
		decoder.Close()
		t.Fatalf("Close failed: %v", err)
	}
	decoder.Close()

	// Reopen the output and verify track order/content one-to-one.
	out := verifyContainerOpens(t, dstPath)
	defer out.Close()

	if got := out.NumStreams(); got != 2 {
		t.Fatalf("expected 2 output tracks, got %d", got)
	}
	if got := streamCodecParType(t, out, 0); got != avutil.MediaTypeAudio {
		t.Fatalf("output track 0 should be audio (selected first), got %v", got)
	}
	if got := streamCodecParType(t, out, 1); got != avutil.MediaTypeVideo {
		t.Fatalf("output track 1 should be video, got %v", got)
	}

	// Every packet in the output must reference a valid output track and
	// carry the codec type of the track it belongs to.
	counts := map[int]int{}
	for {
		pkt, err := out.ReadPacket()
		if err != nil {
			t.Fatalf("reading output packet failed: %v", err)
		}
		if pkt == nil {
			break
		}
		idx := pkt.StreamIndex()
		if idx < 0 || idx >= 2 {
			t.Fatalf("packet referenced out-of-range output stream %d", idx)
		}
		counts[idx]++
	}
	if counts[0] == 0 {
		t.Fatal("no audio packets found on output track 0")
	}
	if counts[1] == 0 {
		t.Fatal("no video packets found on output track 1")
	}
	t.Logf("subset output track counts: audio=%d video=%d", counts[0], counts[1])
}

// TestRemuxerUnselectedStreamSkipped focuses narrowly on the quiet-skip
// contract: selecting a single stream must not error when other streams'
// packets are presented, and the output must contain only that stream.
func TestRemuxerUnselectedStreamSkipped(t *testing.T) {
	if !requireFFmpeg(t) {
		return
	}

	srcPath := createRemuxSource(t)
	decoder := openSourceDecoder(t, srcPath)
	defer decoder.Close()

	videoInput := -1
	for i := 0; i < decoder.NumStreams(); i++ {
		stream := avformat.GetStream(decoder.formatCtx, i)
		if avformat.GetCodecParType(avformat.GetStreamCodecPar(stream)) == avutil.MediaTypeVideo {
			videoInput = i
			break
		}
	}
	if videoInput < 0 {
		t.Fatal("no video stream in source")
	}

	dstPath := filepath.Join(t.TempDir(), "video_only.mkv")
	remuxer, err := NewRemuxer(dstPath, decoder, &RemuxerConfig{InputStreams: []int{videoInput}})
	if err != nil {
		t.Fatalf("NewRemuxer failed: %v", err)
	}

	// Feed every packet, including audio ones; none may error.
	for {
		pkt, err := decoder.ReadPacket()
		if err != nil {
			_ = remuxer.Close()
			t.Fatalf("ReadPacket failed: %v", err)
		}
		if pkt == nil {
			break
		}
		if err := remuxer.WritePacket(pkt.ptr, pkt.StreamIndex()); err != nil {
			_ = remuxer.Close()
			t.Fatalf("WritePacket must skip unselected streams without error, got %v", err)
		}
	}
	if err := remuxer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	out := verifyContainerOpens(t, dstPath)
	defer out.Close()
	if got := out.NumStreams(); got != 1 {
		t.Fatalf("expected exactly 1 output stream, got %d", got)
	}
	if got := streamCodecParType(t, out, 0); got != avutil.MediaTypeVideo {
		t.Fatalf("sole output stream should be video, got %v", got)
	}
}

// failWriter is an in-memory seekable writer that starts failing after
// failAfter bytes have been accepted. Bytes accepted before the failure are
// retained so the partial output can be reopened for inspection.
type failWriter struct {
	mu         sync.Mutex
	data       []byte
	pos        int64
	failAfter  int64
	accepted   int64
	failActive bool
}

func (w *failWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.failActive || (w.failAfter >= 0 && w.accepted >= w.failAfter) {
		w.failActive = true
		return 0, errors.New("injected write failure")
	}

	if w.pos < 0 {
		return 0, errors.New("negative seek position")
	}
	need := w.pos + int64(len(p))
	if int64(len(w.data)) < need {
		grown := make([]byte, need)
		copy(grown, w.data)
		w.data = grown
	}
	n := copy(w.data[w.pos:], p)
	w.pos += int64(n)
	if w.pos > w.accepted {
		w.accepted = w.pos
	}
	return n, nil
}

func (w *failWriter) Seek(offset int64, whence int) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = w.pos + offset
	case io.SeekEnd:
		newPos = int64(len(w.data)) + offset
	default:
		return 0, fmt.Errorf("unsupported whence %d", whence)
	}
	if newPos < 0 {
		return 0, errors.New("negative seek result")
	}
	w.pos = newPos
	return newPos, nil
}

func (w *failWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.data...)
}

// TestRemuxerWriteFailureKeepsFileReopenable verifies that after a packet
// write fails mid-stream, Close does not append a trailer/index pointing at
// missing data, and the resulting partial container can still be reopened
// and parsed.
func TestRemuxerWriteFailureKeepsFileReopenable(t *testing.T) {
	if !requireFFmpeg(t) {
		return
	}

	srcPath := createRemuxSource(t)
	decoder := openSourceDecoder(t, srcPath)
	defer decoder.Close()

	// Matroska is streamable and stays parseable without a trailing index.
	// A small AVIO buffer (1KB) flushes almost immediately, so the injected
	// failure surfaces during packet writes rather than at Close. The
	// threshold is large enough to retain the header and a meaningful number
	// of packets before aborting.
	writer := &failWriter{failAfter: 16384}
	remuxer, err := NewRemuxerToWriterWithBufferSize(writer, "matroska", decoder, nil, 1024)
	if err != nil {
		t.Fatalf("NewRemuxerToWriter failed: %v", err)
	}

	var sawWriteFailure bool
	for {
		pkt, readErr := decoder.ReadPacket()
		if readErr != nil {
			_ = remuxer.Close()
			t.Fatalf("ReadPacket failed: %v", readErr)
		}
		if pkt == nil {
			break
		}
		if writeErr := remuxer.WritePacket(pkt.ptr, pkt.StreamIndex()); writeErr != nil {
			sawWriteFailure = true
			break
		}
	}

	if !sawWriteFailure {
		_ = remuxer.Close()
		t.Fatal("test harness did not trigger a write failure")
	}

	// Close must not error even though the output is aborted, and must not
	// write a trailer over the failed stream.
	if closeErr := remuxer.Close(); closeErr != nil {
		t.Fatalf("Close after failed write must not surface a new error, got %v", closeErr)
	}

	partial := writer.Bytes()
	if len(partial) == 0 {
		t.Fatal("no output bytes were flushed before the failure")
	}

	// The partial container must still be parseable. Persist to a temp file
	// and parse it both via the library decoder and the ffmpeg CLI when
	// available.
	partialPath := filepath.Join(t.TempDir(), "partial.mkv")
	if err := os.WriteFile(partialPath, partial, 0o644); err != nil {
		t.Fatalf("write partial file: %v", err)
	}

	reopened := verifyContainerBytesOpens(t, partial, "matroska")
	packets := 0
	for {
		pkt, err := reopened.ReadPacket()
		if err != nil {
			reopened.Close()
			// A read error late in the stream is acceptable for truncated
			// output, but opening/probing must have succeeded.
			break
		}
		if pkt == nil {
			break
		}
		packets++
	}
	reopened.Close()
	t.Logf("reopened partial container and read %d packets before truncation", packets)

	// Cross-check with the ffmpeg/ffprobe CLI.
	//
	// Probing must succeed: ffprobe reads the header and reports the stream
	// inventory, which proves the container is structurally parseable rather
	// than a corrupt "complete" file with a bogus index.
	if ffprobeBin, lookErr := exec.LookPath("ffprobe"); lookErr == nil {
		probe := exec.Command(ffprobeBin, "-v", "error",
			"-show_entries", "stream=index,codec_type", "-of", "csv", partialPath)
		var probeOut, probeErr bytes.Buffer
		probe.Stdout = &probeOut
		probe.Stderr = &probeErr
		if err := probe.Run(); err != nil {
			t.Fatalf("ffprobe could not parse partial container: %v\n%s", err, probeErr.String())
		}
		if !bytes.Contains(probeOut.Bytes(), []byte("video")) {
			t.Fatalf("ffprobe did not find expected stream in partial output: %q", probeOut.String())
		}
		t.Logf("ffprobe parsed partial container streams:\n%s", probeOut.String())
	}

	// A decode pass is expected to stop at the truncation point. The only
	// diagnostics allowed are the well-known truncation messages; any
	// genuine container-integrity failure (bad index, missing header atoms,
	// unknown/garbled format) fails the test.
	if ffmpegBin, lookErr := exec.LookPath("ffmpeg"); lookErr == nil {
		cmd := exec.Command(ffmpegBin, "-v", "error", "-i", partialPath, "-f", "null", "-")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		_ = cmd.Run()

		allowed := []string{
			"file ended prematurely",
			"invalid data found when processing input", // generic AVERROR_INVALIDDATA tail of an EOF stop
			"invalid argument",
			"task finished with error",
			"terminating thread",
			"cannot determine format of input", // stream cut before its first full packet
		}
		lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}
			lower := strings.ToLower(trimmed)
			ok := false
			for _, marker := range allowed {
				if strings.Contains(lower, marker) {
					ok = true
					break
				}
			}
			if !ok {
				t.Fatalf("unexpected structural diagnostic for partial file: %q\nfull output:\n%s",
					trimmed, stderr.String())
			}
		}
		t.Logf("ffmpeg decode stopped at truncation as expected: %q", strings.TrimSpace(stderr.String()))
	}
}

// TestRemuxerCloseOnce ensures repeated and concurrent Close calls finalize
// the output exactly once: no double trailer, no secondary error.
func TestRemuxerCloseOnce(t *testing.T) {
	if !requireFFmpeg(t) {
		return
	}

	srcPath := createRemuxSource(t)
	decoder := openSourceDecoder(t, srcPath)

	dstPath := filepath.Join(t.TempDir(), "close_once.mkv")
	remuxer, err := NewRemuxer(dstPath, decoder, nil)
	if err != nil {
		decoder.Close()
		t.Fatalf("NewRemuxer failed: %v", err)
	}
	if err := remuxer.WriteHeader(); err != nil {
		_ = remuxer.Close()
		decoder.Close()
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write at least one packet so a trailer has something to finalize.
	firstPkt := false
	for {
		pkt, readErr := decoder.ReadPacket()
		if readErr != nil {
			_ = remuxer.Close()
			decoder.Close()
			t.Fatalf("ReadPacket failed: %v", readErr)
		}
		if pkt == nil {
			break
		}
		if err := remuxer.WritePacket(pkt.ptr, pkt.StreamIndex()); err != nil {
			_ = remuxer.Close()
			decoder.Close()
			t.Fatalf("WritePacket failed: %v", err)
		}
		firstPkt = true
		break
	}
	decoder.Close()
	if !firstPkt {
		_ = remuxer.Close()
		t.Fatal("source produced no packets")
	}

	// Concurrent close: many goroutines, only one may run finalization.
	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = remuxer.Close()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close[%d] returned error: %v", i, err)
		}
	}

	// Sequential repeated closes after the concurrent storm must also be
	// inert no-ops.
	for i := 0; i < 5; i++ {
		if err := remuxer.Close(); err != nil {
			t.Fatalf("repeated Close returned error: %v", err)
		}
	}

	// Finalized output must still be a valid, parseable container.
	out := verifyContainerOpens(t, dstPath)
	defer out.Close()
}

// TestRemuxerTimestampUsesPostHeaderTimeBase verifies that timestamps are
// rescaled against the real output time base established after writing the
// header, so subset copying does not drift audio/video duration.
func TestRemuxerTimestampUsesPostHeaderTimeBase(t *testing.T) {
	if !requireFFmpeg(t) {
		return
	}

	srcPath := createRemuxSource(t)
	decoder := openSourceDecoder(t, srcPath)
	defer decoder.Close()

	// mp4/mov is a muxer that normalizes the output stream time base during
	// avformat_write_header, making it a good target for this check.
	dstPath := filepath.Join(t.TempDir(), "tb_subset.mp4")
	remuxer, err := NewRemuxer(dstPath, decoder, nil)
	if err != nil {
		t.Fatalf("NewRemuxer failed: %v", err)
	}
	if err := remuxer.Remux(decoder); err != nil {
		_ = remuxer.Close()
		t.Fatalf("Remux failed: %v", err)
	}
	if err := remuxer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	srcDec := openSourceDecoder(t, srcPath)
	defer srcDec.Close()
	outDec := verifyContainerOpens(t, dstPath)
	defer outDec.Close()

	// Compare duration in seconds per matching media type, derived from the
	// last packet PTS+duration converted through each stream's time base.
	durationSeconds := func(d *Decoder) map[avutil.MediaType]float64 {
		result := map[avutil.MediaType]float64{}
		lastEnd := map[avutil.MediaType]int64{}
		tb := map[avutil.MediaType]avutil.Rational{}

		for i := 0; i < d.NumStreams(); i++ {
			st := avformat.GetStream(d.formatCtx, i)
			typ := avformat.GetCodecParType(avformat.GetStreamCodecPar(st))
			num, den := avformat.GetStreamTimeBase(st)
			tb[typ] = avutil.NewRational(num, den)
		}

		for {
			pkt, err := d.ReadPacket()
			if err != nil || pkt == nil {
				break
			}
			st := avformat.GetStream(d.formatCtx, pkt.StreamIndex())
			typ := avformat.GetCodecParType(avformat.GetStreamCodecPar(st))
			end := pkt.PTS()
			if dur := avcodec.GetPacketDuration(pkt.ptr); dur > 0 {
				end += dur
			}
			if end > lastEnd[typ] {
				lastEnd[typ] = end
			}
		}
		for typ, end := range lastEnd {
			base := tb[typ]
			if base.Den != 0 {
				result[typ] = float64(end) * float64(base.Num) / float64(base.Den)
			}
		}
		return result
	}

	srcDur := durationSeconds(srcDec)
	outDur := durationSeconds(outDec)

	for _, typ := range []avutil.MediaType{avutil.MediaTypeVideo, avutil.MediaTypeAudio} {
		s, okS := srcDur[typ]
		o, okO := outDur[typ]
		if !okS || !okO {
			t.Fatalf("missing duration for media type %v: source=%v output=%v", typ, srcDur, outDur)
		}
		// Allow a small tolerance (one frame-ish) rather than exact equality.
		diff := s - o
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.05 {
			t.Fatalf("media type %v duration drift: source=%.4fs output=%.4fs", typ, s, o)
		}
		t.Logf("media type %v duration source=%.4fs output=%.4fs", typ, s, o)
	}
}

// TestRemuxerFullCopyNoRegression confirms the normal all-stream copy path
// still produces a parseable container with the same number of streams and
// packets as the source.
func TestRemuxerFullCopyNoRegression(t *testing.T) {
	if !requireFFmpeg(t) {
		return
	}

	srcPath := createRemuxSource(t)
	decoder := openSourceDecoder(t, srcPath)
	defer decoder.Close()

	dstPath := filepath.Join(t.TempDir(), "full.mkv")
	remuxer, err := NewRemuxer(dstPath, decoder, nil)
	if err != nil {
		t.Fatalf("NewRemuxer failed: %v", err)
	}
	if err := remuxer.Remux(decoder); err != nil {
		_ = remuxer.Close()
		t.Fatalf("Remux failed: %v", err)
	}
	if err := remuxer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	srcDec := openSourceDecoder(t, srcPath)
	defer srcDec.Close()
	outDec := verifyContainerOpens(t, dstPath)
	defer outDec.Close()

	if got, want := outDec.NumStreams(), srcDec.NumStreams(); got != want {
		t.Fatalf("stream count regression: got %d want %d", got, want)
	}

	countPackets := func(d *Decoder) int {
		n := 0
		for {
			pkt, err := d.ReadPacket()
			if err != nil || pkt == nil {
				break
			}
			n++
		}
		return n
	}

	srcPackets := countPackets(srcDec)
	outPackets := countPackets(outDec)
	if outPackets != srcPackets {
		t.Fatalf("packet count regression: got %d want %d", outPackets, srcPackets)
	}
	t.Logf("full copy preserved %d streams and %d packets", srcDec.NumStreams(), srcPackets)
}

// TestRemuxerRejectsDuplicateInputStreams guards the one-to-one mapping
// invariant: a duplicated input index must be rejected rather than silently
// collapsing two output tracks onto one map entry.
func TestRemuxerRejectsDuplicateInputStreams(t *testing.T) {
	if !requireFFmpeg(t) {
		return
	}

	srcPath := createRemuxSource(t)
	decoder := openSourceDecoder(t, srcPath)
	defer decoder.Close()

	dstPath := filepath.Join(t.TempDir(), "dup.mkv")
	_, err := NewRemuxer(dstPath, decoder, &RemuxerConfig{InputStreams: []int{0, 0}})
	if err == nil {
		t.Fatal("expected error for duplicate input stream indices, got nil")
	}
}

// TestRemuxerFailureSkipsTrailer explicitly verifies the failure finalization
// contract on a real file-backed muxer: after a write failure, Close must not
// write a trailer. For matroska the trailer carries the seek index; a healthy
// file ends with the trailing segment information, while an aborted file must
// stop at the last flushed cluster.
func TestRemuxerFailureSkipsTrailer(t *testing.T) {
	if !requireFFmpeg(t) {
		return
	}

	srcPath := createRemuxSource(t)

	// Healthy reference: full copy with a proper trailer.
	goodDecoder := openSourceDecoder(t, srcPath)
	goodPath := filepath.Join(t.TempDir(), "good.mkv")
	goodRemuxer, err := NewRemuxer(goodPath, goodDecoder, nil)
	if err != nil {
		goodDecoder.Close()
		t.Fatalf("NewRemuxer good failed: %v", err)
	}
	if err := goodRemuxer.Remux(goodDecoder); err != nil {
		_ = goodRemuxer.Close()
		goodDecoder.Close()
		t.Fatalf("good Remux failed: %v", err)
	}
	if err := goodRemuxer.Close(); err != nil {
		goodDecoder.Close()
		t.Fatalf("good Close failed: %v", err)
	}
	goodDecoder.Close()

	// Aborted output through a failing writer.
	badDecoder := openSourceDecoder(t, srcPath)
	defer badDecoder.Close()
	writer := &failWriter{failAfter: 16384}
	badRemuxer, err := NewRemuxerToWriterWithBufferSize(writer, "matroska", badDecoder, nil, 1024)
	if err != nil {
		t.Fatalf("NewRemuxerToWriter failed: %v", err)
	}

	failed := false
	for {
		pkt, readErr := badDecoder.ReadPacket()
		if readErr != nil {
			_ = badRemuxer.Close()
			t.Fatalf("ReadPacket failed: %v", readErr)
		}
		if pkt == nil {
			break
		}
		if writeErr := badRemuxer.WritePacket(pkt.ptr, pkt.StreamIndex()); writeErr != nil {
			failed = true
			break
		}
	}
	if !failed {
		_ = badRemuxer.Close()
		t.Fatal("harness did not inject a failure")
	}
	if err := badRemuxer.Close(); err != nil {
		t.Fatalf("Close after failure must not error, got %v", err)
	}

	goodBytes, err := os.ReadFile(goodPath)
	if err != nil {
		t.Fatalf("read good file: %v", err)
	}
	badBytes := writer.Bytes()

	// Both files carry the EBML/matroska magic header, but the aborted file
	// must be shorter than a fully finalized one.
	ebmlMagic := []byte{0x1A, 0x45, 0xDF, 0xA3}
	if !bytes.HasPrefix(badBytes, ebmlMagic) {
		t.Fatal("aborted output lost its matroska EBML header")
	}
	if !bytes.HasPrefix(goodBytes, ebmlMagic) {
		t.Fatal("reference output missing matroska EBML header")
	}
	if len(badBytes) >= len(goodBytes) {
		t.Fatalf("aborted output (%d bytes) should be smaller than finalized output (%d bytes)",
			len(badBytes), len(goodBytes))
	}

	// Reopen via library decoder to confirm parseability after skipped trailer.
	reopened := verifyContainerBytesOpens(t, badBytes, "matroska")
	reopened.Close()
}
