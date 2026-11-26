package dca

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"
)

// TestDecode keeps the original behaviour test: the bundled sample must still
// decode completely into 755 audio frames.
func TestDecode(t *testing.T) {
	file, err := os.Open("testaudio.dca")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	decoder := NewDecoder(file)

	err = decoder.ReadMetadata()
	if err != nil {
		t.Fatal(err)
	}

	if decoder.Metadata == nil || decoder.Metadata.Opus == nil {
		t.Fatal("metadata not decoded")
	}

	frameCounter := 0
	for {
		_, err := decoder.OpusFrame()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}
		frameCounter++
	}

	if frameCounter != 755 {
		t.Errorf("incorrect number of frames: got %d expected 755", frameCounter)
	}
}

// TestDecodeAutoMetadata checks the implicit metadata detection in OpusFrame.
func TestDecodeAutoMetadata(t *testing.T) {
	data, err := os.ReadFile("testaudio.dca")
	if err != nil {
		t.Fatal(err)
	}

	decoder := NewDecoder(bytes.NewReader(data))
	if _, err := decoder.OpusFrame(); err != nil {
		t.Fatalf("first OpusFrame failed: %v", err)
	}
	if decoder.Metadata == nil {
		t.Error("OpusFrame did not read the metadata")
	}
}

// TestDecodeFrameDuration checks FrameDuration on valid and malformed metadata.
func TestDecodeFrameDuration(t *testing.T) {
	valid := &Decoder{Metadata: &Metadata{Opus: &OpusMetadata{FrameSize: 1920, Channels: 2}}}
	if got := valid.FrameDuration(); got <= 0 {
		t.Errorf("FrameDuration = %v, expected > 0", got)
	}

	malformed := []*Metadata{
		nil,
		{},
		{Opus: &OpusMetadata{}},
		{Opus: &OpusMetadata{Channels: 0, FrameSize: 1920}},
		{Opus: &OpusMetadata{Channels: 2, FrameSize: 0}},
		{Opus: &OpusMetadata{Channels: 2, FrameSize: 1}},
	}

	for i, md := range malformed {
		d := &Decoder{Metadata: md}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("case %d: FrameDuration panicked: %v", i, r)
				}
			}()
			if got := d.FrameDuration(); got != 20*1e6 {
				t.Logf("case %d: FrameDuration = %v", i, got)
			}
		}()
	}
}

// buildMetadataStream builds a raw "DCA1" prefix with the given metadata length
// but only len(payload) bytes of payload, plus an optional trailing audio frame.
func buildMetadataStream(metaLen int32, payload []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("DCA1")
	_ = binary.Write(&buf, binary.LittleEndian, metaLen)
	buf.Write(payload)
	return buf.Bytes()
}

// TestDecodeNegativeMetadataLength checks that a negative metadata length is
// rejected instead of turning into a huge allocation.
func TestDecodeNegativeMetadataLength(t *testing.T) {
	stream := buildMetadataStream(-1, nil)

	decoder := NewDecoder(bytes.NewReader(stream))
	err := decoder.ReadMetadata()
	if err == nil {
		t.Fatal("expected an error for a negative metadata length")
	}
	if !errors.Is(err, ErrBadMetadata) {
		t.Errorf("expected ErrBadMetadata, got %v", err)
	}
}

// TestDecodeHugeMetadataLength checks the sanity limit for absurd lengths.
func TestDecodeHugeMetadataLength(t *testing.T) {
	stream := buildMetadataStream(1<<30, []byte("{}"))

	decoder := NewDecoder(bytes.NewReader(stream))
	err := decoder.ReadMetadata()
	if err == nil {
		t.Fatal("expected an error for an oversized metadata length")
	}
	if !errors.Is(err, ErrBadMetadata) {
		t.Errorf("expected ErrBadMetadata, got %v", err)
	}
}

// TestDecodeTruncatedMetadata checks that a stream which promises more metadata
// bytes than it has returns an error.
func TestDecodeTruncatedMetadata(t *testing.T) {
	stream := buildMetadataStream(1024, []byte(`{"dca":`))

	decoder := NewDecoder(bytes.NewReader(stream))
	err := decoder.ReadMetadata()
	if err == nil {
		t.Fatal("expected an error for truncated metadata")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

// TestDecodeTruncatedLengthField checks a stream that ends inside the length
// field of the metadata header.
func TestDecodeTruncatedLengthField(t *testing.T) {
	stream := []byte("DCA1\x01\x00")

	decoder := NewDecoder(bytes.NewReader(stream))
	if err := decoder.ReadMetadata(); err == nil {
		t.Fatal("expected an error for a truncated metadata length")
	}
}

// TestDecodeMetadataNotFirstFrame checks the sequencing rule of the decoder.
func TestDecodeMetadataNotFirstFrame(t *testing.T) {
	decoder := NewDecoder(bytes.NewReader(nil))
	if err := decoder.ReadMetadata(); err == nil {
		t.Fatal("expected an error reading metadata from an empty stream")
	}
	if err := decoder.ReadMetadata(); !errors.Is(err, ErrNotFirstFrame) {
		t.Errorf("expected ErrNotFirstFrame on the second call, got %v", err)
	}
}

// TestDecodeNegativeFrameSize checks DecodeFrame rejects a negative size.
func TestDecodeNegativeFrameSize(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, int16(-1))
	buf.Write([]byte("queued data"))

	_, err := DecodeFrame(&buf)
	if !errors.Is(err, ErrNegativeFrameSize) {
		t.Fatalf("expected ErrNegativeFrameSize, got %v", err)
	}
}

// TestDecodeTruncatedFrame checks a frame whose declared length exceeds the
// available bytes.
func TestDecodeTruncatedFrame(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, int16(64))
	buf.Write([]byte{1, 2, 3})

	if _, err := DecodeFrame(&buf); err == nil {
		t.Fatal("expected an error for a truncated frame")
	}
}

// TestDecodeTruncatedFrameLength checks a stream ending inside the frame size
// field itself.
func TestDecodeTruncatedFrameLength(t *testing.T) {
	if _, err := DecodeFrame(bytes.NewReader([]byte{0x01})); err != io.ErrUnexpectedEOF && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestDecodeEmptyFrame checks that a zero length frame is valid but empty.
func TestDecodeEmptyFrame(t *testing.T) {
	frame, err := DecodeFrame(bytes.NewReader([]byte{0x00, 0x00}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frame) != 0 {
		t.Errorf("expected an empty frame, got %d bytes", len(frame))
	}
}

// TestOpusFrameNegativeSize checks that a negative frame length inside a
// stream surfaces as an error rather than an allocation.
func TestOpusFrameNegativeSize(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(buildMetadataStream(2, []byte("{}")))
	_ = binary.Write(&buf, binary.LittleEndian, int16(-32768))

	decoder := NewDecoder(bytes.NewReader(buf.Bytes()))
	if _, err := decoder.OpusFrame(); !errors.Is(err, ErrNegativeFrameSize) {
		t.Fatalf("expected ErrNegativeFrameSize, got %v", err)
	}
}

// TestOpusFrameSkipsMetadata makes sure the metadata frame of an encoded
// stream is not handed to the caller as audio.
func TestOpusFrameSkipsMetadata(t *testing.T) {
	meta := buildMetadataStream(2, []byte("{}"))

	var buf bytes.Buffer
	buf.Write(meta)

	var frame bytes.Buffer
	_ = binary.Write(&frame, binary.LittleEndian, int16(4))
	frame.Write([]byte("opus"))
	buf.Write(frame.Bytes())

	decoder := NewDecoder(bytes.NewReader(buf.Bytes()))
	opus, err := decoder.OpusFrame()
	if err != nil {
		t.Fatalf("OpusFrame failed: %v", err)
	}
	if string(opus) != "opus" {
		t.Errorf("got %q, expected the audio payload", opus)
	}
}
