//go:build !ios && !android && (amd64 || arm64)

package ffgo

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/obinnaokechukwu/ffgo/avcodec"
	"github.com/obinnaokechukwu/ffgo/avformat"
	"github.com/obinnaokechukwu/ffgo/avutil"
	"github.com/obinnaokechukwu/ffgo/internal/bindings"
)

// Remuxer copies streams from input to output without re-encoding.
// This is useful for changing container formats or extracting streams.
type Remuxer struct {
	mu sync.Mutex

	// Output context
	outputCtx avformat.FormatContext
	outputIO  avformat.IOContext

	// customIO is non-nil when the output is backed by user-supplied
	// I/O callbacks. It owns the AVIOContext and must be released with
	// IOContextFree (instead of avio_closep).
	customIO *CustomIOContext

	// Stream mapping: inputStreamIdx -> outputStreamIdx.
	// Output indices are assigned sequentially in the order of the
	// requested input streams, so the output track order always matches
	// the order of InputStreams in RemuxerConfig one-to-one.
	streamMap map[int]int

	// orderedInputs preserves the requested input stream order so that
	// callers can reason about output track positions deterministically.
	orderedInputs []int

	// Time base mapping for timestamp rescaling.
	// outputTimeBases is refreshed from the real output streams after
	// the container header is written, because some muxers only fix the
	// stream time base during avformat_write_header.
	inputTimeBases  map[int]avutil.Rational
	outputTimeBases map[int]avutil.Rational

	// Reusable packet
	packet avcodec.Packet

	headerWritten bool
	writeFailed   bool
	closed        bool
}

// RemuxerConfig configures a remuxer.
type RemuxerConfig struct {
	// InputStreams specifies which input stream indices to copy.
	// If empty, all streams are copied.
	InputStreams []int
}

// NewRemuxer creates a new remuxer that copies packets from decoder to output file.
// The decoder is used to get input stream information.
func NewRemuxer(outputPath string, decoder *Decoder, cfg *RemuxerConfig) (*Remuxer, error) {
	if decoder == nil {
		return nil, errors.New("ffgo: decoder is required for remuxing")
	}

	if err := bindings.Load(); err != nil {
		return nil, err
	}

	r := &Remuxer{
		streamMap:       make(map[int]int),
		inputTimeBases:  make(map[int]avutil.Rational),
		outputTimeBases: make(map[int]avutil.Rational),
	}

	// Determine output format from filename
	formatName := guessFormatFromPath(outputPath)
	if formatName == "" {
		return nil, errors.New("ffgo: cannot determine output format from filename")
	}

	// Create output format context
	if err := avformat.AllocOutputContext2(&r.outputCtx, nil, formatName, outputPath); err != nil {
		return nil, err
	}

	if err := r.initStreams(decoder, cfg); err != nil {
		r.cleanup()
		return nil, err
	}

	// Open output file if needed
	if !avformat.HasNoFile(r.outputCtx) {
		if err := avformat.IOOpen(&r.outputIO, outputPath, avformat.IOFlagWrite); err != nil {
			r.cleanup()
			return nil, err
		}
		avformat.SetIOContext(r.outputCtx, r.outputIO)
	}

	// Allocate packet
	r.packet = avcodec.PacketAlloc()
	if r.packet == nil {
		r.cleanup()
		return nil, errors.New("ffgo: failed to allocate packet")
	}

	return r, nil
}

// NewRemuxerToWriter creates a remuxer whose output is written through the
// supplied io.Writer instead of a file path. If w also implements io.Seeker,
// seek callbacks are wired up as well. format is the muxer short name
// (e.g. "mp4", "matroska").
//
// This is useful for streaming into memory, network sinks, or test harnesses
// that need to inject write failures.
func NewRemuxerToWriter(w io.Writer, format string, decoder *Decoder, cfg *RemuxerConfig) (*Remuxer, error) {
	return NewRemuxerToWriterWithBufferSize(w, format, decoder, cfg, defaultIOBufferSize)
}

// NewRemuxerToWriterWithBufferSize is NewRemuxerToWriter with an explicit
// AVIO buffer size. A small buffer makes write failures surface promptly
// (the default 32KB buffer can absorb an entire short clip before flushing).
func NewRemuxerToWriterWithBufferSize(w io.Writer, format string, decoder *Decoder, cfg *RemuxerConfig, bufferSize int) (*Remuxer, error) {
	if w == nil {
		return nil, errors.New("ffgo: writer cannot be nil")
	}
	if decoder == nil {
		return nil, errors.New("ffgo: decoder is required for remuxing")
	}
	if format == "" {
		return nil, errors.New("ffgo: output format is required for writer-backed remuxing")
	}
	if bufferSize <= 0 {
		bufferSize = defaultIOBufferSize
	}

	if err := bindings.Load(); err != nil {
		return nil, err
	}

	callbacks := &IOCallbacks{
		Write: func(buf []byte) (int, error) {
			return w.Write(buf)
		},
	}
	if seeker, ok := w.(io.Seeker); ok {
		callbacks.Seek = func(offset int64, whence int) (int64, error) {
			return seeker.Seek(offset, whence)
		}
	}

	ioCtx, err := NewCustomIOContextWithSize(callbacks, true, bufferSize)
	if err != nil {
		return nil, err
	}

	r := &Remuxer{
		streamMap:       make(map[int]int),
		inputTimeBases:  make(map[int]avutil.Rational),
		outputTimeBases: make(map[int]avutil.Rational),
		customIO:        ioCtx,
	}

	if err := avformat.AllocOutputContext2(&r.outputCtx, nil, format, ""); err != nil {
		ioCtx.Close()
		return nil, err
	}
	avformat.SetIOContext(r.outputCtx, ioCtx.AVIOContext())

	if err := r.initStreams(decoder, cfg); err != nil {
		r.cleanup()
		return nil, err
	}

	r.packet = avcodec.PacketAlloc()
	if r.packet == nil {
		r.cleanup()
		return nil, errors.New("ffgo: failed to allocate packet")
	}

	return r, nil
}

// initStreams builds the output streams and the stable input->output mapping.
// Output stream indices are assigned sequentially in requested input order.
func (r *Remuxer) initStreams(decoder *Decoder, cfg *RemuxerConfig) error {
	var streamsToCopy []int
	if cfg != nil && len(cfg.InputStreams) > 0 {
		streamsToCopy = append(streamsToCopy, cfg.InputStreams...)
	} else {
		// Copy all streams in their natural order.
		numStreams := avformat.GetNbStreams(decoder.formatCtx)
		streamsToCopy = make([]int, numStreams)
		for i := 0; i < numStreams; i++ {
			streamsToCopy[i] = i
		}
	}

	r.orderedInputs = append(r.orderedInputs, streamsToCopy...)

	// Reject duplicate input indices: a duplicate would collapse two output
	// tracks onto one map entry and break the one-to-one correspondence.
	seen := make(map[int]struct{}, len(streamsToCopy))
	for _, inputIdx := range streamsToCopy {
		if _, dup := seen[inputIdx]; dup {
			return fmt.Errorf("ffgo: duplicate input stream index %d in remuxer config", inputIdx)
		}
		seen[inputIdx] = struct{}{}
	}

	outputStreamIdx := 0
	for _, inputIdx := range streamsToCopy {
		inputStream := avformat.GetStream(decoder.formatCtx, inputIdx)
		if inputStream == nil {
			return errors.New("ffgo: invalid input stream index")
		}

		// Create output stream
		outputStream := avformat.NewStream(r.outputCtx, nil)
		if outputStream == nil {
			return errors.New("ffgo: failed to create output stream")
		}

		// Copy codec parameters from input to output
		inputCodecPar := avformat.GetStreamCodecPar(inputStream)
		outputCodecPar := avformat.GetStreamCodecPar(outputStream)
		if err := avcodec.ParametersCopy(outputCodecPar, inputCodecPar); err != nil {
			return err
		}

		// Clear codec tag for compatibility with different containers
		avcodec.SetCodecParTag(outputCodecPar, 0)

		// Stable mapping: selected input index -> sequential output index.
		r.streamMap[inputIdx] = outputStreamIdx

		inTbNum, inTbDen := avformat.GetStreamTimeBase(inputStream)
		r.inputTimeBases[inputIdx] = avutil.NewRational(inTbNum, inTbDen)

		// Initial value only; the authoritative output time base is read
		// back from the stream after WriteHeader.
		outTbNum, outTbDen := avformat.GetStreamTimeBase(outputStream)
		r.outputTimeBases[inputIdx] = avutil.NewRational(outTbNum, outTbDen)

		outputStreamIdx++
	}

	return nil
}

// refreshOutputTimeBases reads the real output stream time bases after the
// header has been written. Several muxers (notably mov/mp4) rewrite the
// stream time base in avformat_write_header, so any value captured at
// construction time can be stale and would cause timestamp drift.
func (r *Remuxer) refreshOutputTimeBases() {
	for _, inputIdx := range r.orderedInputs {
		outputIdx := r.streamMap[inputIdx]
		outputStream := avformat.GetStream(r.outputCtx, outputIdx)
		if outputStream != nil {
			tbNum, tbDen := avformat.GetStreamTimeBase(outputStream)
			r.outputTimeBases[inputIdx] = avutil.NewRational(tbNum, tbDen)
		}
	}
}

// WriteHeader writes the output file header.
// Must be called before WritePacket; WritePacket will also trigger it
// automatically on the first packet.
func (r *Remuxer) WriteHeader() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("ffgo: remuxer is closed")
	}
	if r.headerWritten {
		return nil
	}

	if err := avformat.WriteHeader(r.outputCtx, nil); err != nil {
		r.writeFailed = true
		return err
	}
	r.headerWritten = true

	// Capture the authoritative output time bases now that the muxer has
	// finalized them. All subsequent packet rescaling uses these values.
	r.refreshOutputTimeBases()

	return nil
}

// WritePacket copies a packet to the output.
// The packet's stream index is remapped from the input stream index supplied
// by the caller to the corresponding output stream. Packets belonging to
// streams that were not selected in RemuxerConfig are silently skipped.
// Timestamps are rescaled from the input time base to the real output time
// base established when the header was written.
func (r *Remuxer) WritePacket(pkt avcodec.Packet, inputStreamIdx int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("ffgo: remuxer is closed")
	}

	// Streams not selected for copy are ignored quietly: no error, no
	// misrouted packet, no effect on output stream numbering.
	outputIdx, ok := r.streamMap[inputStreamIdx]
	if !ok {
		return nil
	}

	if pkt == nil {
		return errors.New("ffgo: packet cannot be nil")
	}

	// Auto-write header on the first accepted packet.
	if !r.headerWritten {
		if err := avformat.WriteHeader(r.outputCtx, nil); err != nil {
			r.writeFailed = true
			return err
		}
		r.headerWritten = true
		r.refreshOutputTimeBases()
	}

	// Once a previous write failed the output is aborted; refuse further
	// writes instead of appending more data to a container we will not be
	// able to finalize consistently.
	if r.writeFailed {
		return errors.New("ffgo: remuxer has a failed write; close it before writing more")
	}

	// Reference the packet (don't copy data, just increment refcount)
	if err := avcodec.PacketRef(r.packet, pkt); err != nil {
		r.writeFailed = true
		return err
	}

	// Set output stream index according to the stable mapping.
	avcodec.SetPacketStreamIndex(r.packet, int32(outputIdx))

	// Rescale timestamps using the post-header output time base.
	inputTB := r.inputTimeBases[inputStreamIdx]
	outputTB := r.outputTimeBases[inputStreamIdx]
	avcodec.RescalePacketTS(r.packet, inputTB, outputTB)

	// Write the packet
	err := avformat.InterleavedWriteFrame(r.outputCtx, r.packet)

	// Unref the packet
	avcodec.PacketUnref(r.packet)

	if err != nil {
		// Mark the output as aborted so Close will not append a trailer
		// whose index could reference data that never made it to disk.
		r.writeFailed = true
		return err
	}

	return nil
}

// Remux copies all packets from a decoder to the output.
// This is a convenience method that reads all packets and writes them.
func (r *Remuxer) Remux(decoder *Decoder) error {
	if err := r.WriteHeader(); err != nil {
		return err
	}

	for {
		pkt, err := decoder.ReadPacket()
		if err != nil {
			return err
		}
		if pkt == nil {
			break
		}

		streamIdx := pkt.StreamIndex()
		if err := r.WritePacket(pkt.ptr, streamIdx); err != nil {
			return err
		}
	}

	return nil
}

// Close finalizes and closes the remuxer. It is safe to call multiple times
// and from multiple goroutines concurrently: exactly one call performs the
// finalization, every other call returns nil without touching the output.
//
// If any packet write failed, the trailer is deliberately not written, so the
// output file never carries an index pointing at missing packets. The already
// flushed data remains on disk and can be reopened and parsed up to the point
// of failure instead of being presented as a corrupt "complete" file.
func (r *Remuxer) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeLocked()
}

// closeLocked contains the finalization logic. The caller must hold r.mu.
// Splitting it out keeps the once-only guarantee in one place for both
// sequential and concurrent Close callers.
func (r *Remuxer) closeLocked() error {
	if r.closed {
		// Finalization already ran (or is running under this same lock);
		// never write the trailer twice or surface a second error.
		return nil
	}
	r.closed = true

	var firstErr error

	// Only finalize a healthy, fully written output. On a failed write the
	// trailer index could reference packets that were never persisted, so
	// skip it deliberately to keep the partial file parseable.
	if r.outputCtx != nil && r.headerWritten && !r.writeFailed {
		if err := avformat.WriteTrailer(r.outputCtx); err != nil {
			firstErr = err
		}
	}

	r.cleanup()
	return firstErr
}

func (r *Remuxer) cleanup() {
	if r.packet != nil {
		avcodec.PacketFree(&r.packet)
	}
	if r.customIO != nil {
		// User-callback AVIOContext: avio_context_free, never avio_closep.
		_ = r.customIO.Close()
		r.customIO = nil
		r.outputIO = nil
	} else if r.outputIO != nil && r.outputCtx != nil {
		_ = avformat.IOCloseP(&r.outputIO)
	}
	if r.outputCtx != nil {
		avformat.FreeContext(r.outputCtx)
		r.outputCtx = nil
	}
}

// StreamMapping returns the mapping from input stream indices to output stream indices.
func (r *Remuxer) StreamMapping() map[int]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Return a copy so callers cannot mutate internal state.
	out := make(map[int]int, len(r.streamMap))
	for k, v := range r.streamMap {
		out[k] = v
	}
	return out
}

// NumOutputStreams returns the number of output streams.
func (r *Remuxer) NumOutputStreams() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streamMap)
}
