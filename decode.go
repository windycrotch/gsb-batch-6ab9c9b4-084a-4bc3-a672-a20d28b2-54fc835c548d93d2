package dca

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"
)

var (
	ErrNotDCA        = errors.New("DCA Magic header not found, either not dca or raw dca frames")
	ErrNotFirstFrame = errors.New("Metadata can only be found in the first frame")
	ErrBadMetadata   = errors.New("Invalid metadata length")
)

// maxMetadataLen is a sanity limit for the metadata block. The metadata is a
// small JSON document (a few KiB, more with an embedded cover image), so
// anything larger is treated as a corrupt stream rather than allocated.
const maxMetadataLen = 8 << 20

type Decoder struct {
	r *bufio.Reader

	Metadata      *Metadata
	FormatVersion int

	// Set to true after the first frame has been read
	firstFrameProcessed bool
}

// NewDecoder returns a new dca decoder
func NewDecoder(r io.Reader) *Decoder {
	decoder := &Decoder{
		r: bufio.NewReader(r),
	}

	return decoder
}

// ReadMetadata reads the first metadata frame
// OpusFrame will call this automatically if
func (d *Decoder) ReadMetadata() error {
	if d.firstFrameProcessed {
		return ErrNotFirstFrame
	}
	d.firstFrameProcessed = true

	fingerprint, err := d.r.Peek(4)
	if err != nil {
		return err
	}

	if string(fingerprint[:3]) != "DCA" {
		return ErrNotDCA
	}

	// We just peeked earlier, mark this portion as read
	d.r.Discard(4)

	// Read the format version
	version, err := strconv.ParseInt(string(fingerprint[3:]), 10, 32)
	if err != nil {
		return err
	}
	d.FormatVersion = int(version)

	// The length of the metadata
	var metaLen int32
	err = binary.Read(d.r, binary.LittleEndian, &metaLen)
	if err != nil {
		return err
	}

	// A negative length would turn into a huge allocation below, and a length
	// larger than the sanity limit can only mean a corrupt stream.
	if metaLen < 0 {
		return ErrBadMetadata
	}
	if int64(metaLen) > maxMetadataLen {
		return ErrBadMetadata
	}

	// Read in the metadata itself. A truncated stream must be reported as such
	// (io.ErrUnexpectedEOF) instead of silently returning a short buffer.
	jsonBuf := make([]byte, metaLen)
	_, err = io.ReadFull(d.r, jsonBuf)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}

	// And unmarshal it
	var metadata *Metadata
	err = json.Unmarshal(jsonBuf, &metadata)
	if err != nil {
		return err
	}
	if metadata == nil {
		return ErrBadMetadata
	}
	d.Metadata = metadata
	return nil
}

// OpusFrame returns the next audio frame
// If this is the first frame it will also check for metadata in it
func (d *Decoder) OpusFrame() (frame []byte, err error) {
	if !d.firstFrameProcessed {
		// Check to see if this contains metadata and read the metadata if so
		magic, err := d.r.Peek(3)
		if err != nil {
			return nil, err
		}

		if string(magic) == "DCA" {
			err = d.ReadMetadata()
			if err != nil {
				return nil, err
			}
		}
	}

	frame, err = DecodeFrame(d.r)
	return
}

// FrameDuration implements OpusReader, returnining the specified duration per frame
func (d *Decoder) FrameDuration() time.Duration {
	defaultDuration := time.Duration(20) * time.Millisecond

	// The metadata is untrusted input, guard against a missing opus block and
	// against a channel count/frame size that would divide by zero.
	if d.Metadata == nil || d.Metadata.Opus == nil {
		return defaultDuration
	}
	if d.Metadata.Opus.Channels <= 0 || d.Metadata.Opus.FrameSize <= 0 {
		return defaultDuration
	}

	pcmPerChannel := d.Metadata.Opus.FrameSize / d.Metadata.Opus.Channels
	if pcmPerChannel <= 0 {
		return defaultDuration
	}

	// I don't understand nick, why does it have to be like this nick, please nick, im not having a good time nick.
	// 960B = pcm framesize of 20ms 1 channel audio
	return time.Duration((pcmPerChannel/960)*20) * time.Millisecond
}
