package rawmessage_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/chunk"
	"github.com/bluenviron/gortmplib/pkg/rawmessage"
)

var cases = []struct {
	name     string
	messages []*rawmessage.Message
	chunks   []chunk.Chunk
}{
	{
		"(chunk0) + (chunk1)",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       18576 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x03}, 64),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15) * time.Millisecond,
				Type:            5,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x04}, 64),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       18576,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         64,
				Body:            bytes.Repeat([]byte{0x03}, 64),
			},
			&chunk.Chunk1{
				ChunkStreamID:  27,
				TimestampDelta: 15,
				Type:           5,
				BodyLen:        64,
				Body:           bytes.Repeat([]byte{0x04}, 64),
			},
		},
	},
	{
		"(chunk0) + (chunk2) + (chunk3)",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       18576 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x03}, 64),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15) * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x04}, 64),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15 + 15) * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x05}, 64),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       18576,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         64,
				Body:            bytes.Repeat([]byte{0x03}, 64),
			},
			&chunk.Chunk2{
				ChunkStreamID:  27,
				TimestampDelta: 15,
				Body:           bytes.Repeat([]byte{0x04}, 64),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x05}, 64),
			},
		},
	},
	{
		"(chunk0 + chunk3) + (chunk1 + chunk3) + (chunk2 + chunk3) + (chunk3 + chunk3)",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       18576 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x03}, 190),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       18576 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x04}, 192),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15) * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x05}, 192),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15 + 15) * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x06}, 192),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       18576,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         190,
				Body:            bytes.Repeat([]byte{0x03}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x03}, 62),
			},
			&chunk.Chunk1{
				ChunkStreamID:  27,
				TimestampDelta: 0,
				Type:           6,
				BodyLen:        192,
				Body:           bytes.Repeat([]byte{0x04}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x04}, 64),
			},
			&chunk.Chunk2{
				ChunkStreamID:  27,
				TimestampDelta: 15,
				Body:           bytes.Repeat([]byte{0x05}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x05}, 64),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x06}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x06}, 64),
			},
		},
	},
	{
		"(chunk0 + chunk3 with extended timestamp)",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       0xFF123456 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{5}, 160),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       4279383126,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         160,
				Body:            bytes.Repeat([]byte{5}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID:     27,
				ExtendedTimestamp: 4279383126,
				Body:              bytes.Repeat([]byte{5}, 32),
			},
		},
	},
	{
		"extended timestamp at sentinel across chunk3 continuations",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       0xFFFFFF * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{5}, 260),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       0xFFFFFF,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         260,
				Body:            bytes.Repeat([]byte{5}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID:     27,
				ExtendedTimestamp: 0xFFFFFF,
				Body:              bytes.Repeat([]byte{5}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID:     27,
				ExtendedTimestamp: 0xFFFFFF,
				Body:              bytes.Repeat([]byte{5}, 4),
			},
		},
	},
	{
		"extended timestamp deltas across chunk formats",
		[]*rawmessage.Message{
			{ChunkStreamID: 27, Type: 6, MessageStreamID: 3123, Body: []byte{1}},
			{ChunkStreamID: 27, Timestamp: 0xFFFFFF * time.Millisecond, Type: 5, MessageStreamID: 3123, Body: []byte{2}},
			{ChunkStreamID: 27, Timestamp: (0xFFFFFF + 1) * time.Millisecond, Type: 4, MessageStreamID: 3123, Body: []byte{3}},
			{ChunkStreamID: 27, Timestamp: (2*0xFFFFFF + 1) * time.Millisecond, Type: 4, MessageStreamID: 3123, Body: []byte{4}},
			{ChunkStreamID: 27, Timestamp: (3*0xFFFFFF + 1) * time.Millisecond, Type: 4, MessageStreamID: 3123, Body: []byte{5}},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{ChunkStreamID: 27, Type: 6, MessageStreamID: 3123, BodyLen: 1, Body: []byte{1}},
			&chunk.Chunk1{ChunkStreamID: 27, TimestampDelta: 0xFFFFFF, Type: 5, BodyLen: 1, Body: []byte{2}},
			&chunk.Chunk1{ChunkStreamID: 27, TimestampDelta: 1, Type: 4, BodyLen: 1, Body: []byte{3}},
			&chunk.Chunk2{ChunkStreamID: 27, TimestampDelta: 0xFFFFFF, Body: []byte{4}},
			&chunk.Chunk3{ChunkStreamID: 27, ExtendedTimestamp: 0xFFFFFF, Body: []byte{5}},
		},
	},
	{
		"decreasing timestamp",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       16 * time.Second,
				Type:            6,
				MessageStreamID: 3123,
				Body:            []byte{1, 2},
			},
			{
				ChunkStreamID:   27,
				Timestamp:       17 * time.Second,
				Type:            6,
				MessageStreamID: 3123,
				Body:            []byte{3, 4},
			},
			{
				ChunkStreamID:   27,
				Timestamp:       16 * time.Second,
				Type:            6,
				MessageStreamID: 3123,
				Body:            []byte{5, 6},
			},
			{
				ChunkStreamID:   27,
				Timestamp:       17 * time.Second,
				Type:            6,
				MessageStreamID: 3123,
				Body:            []byte{7, 8},
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       16000,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         2,
				Body:            []byte{1, 2},
			},
			&chunk.Chunk2{
				ChunkStreamID:  27,
				TimestampDelta: 1000,
				Body:           []byte{3, 4},
			},
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       16000,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         2,
				Body:            []byte{5, 6},
			},
			&chunk.Chunk2{
				ChunkStreamID:  27,
				TimestampDelta: 1000,
				Body:           []byte{7, 8},
			},
		},
	},
}

func TestReader(t *testing.T) {
	for _, ca := range cases { //nolint:dupl
		t.Run(ca.name, func(t *testing.T) {
			var buf bytes.Buffer
			br := bytecounter.NewReader(&buf)
			r := &rawmessage.Reader{
				BR:  br,
				BCR: br,
				OnAckNeeded: func(_ uint32) error {
					return nil
				},
			}
			r.Initialize()

			hasExtendedTimestamp := false

			for _, cach := range ca.chunks {
				buf2, err := cach.Marshal(hasExtendedTimestamp) //nolint:staticcheck
				require.NoError(t, err)
				buf.Write(buf2)
				hasExtendedTimestamp = chunkHasExtendedTimestamp(cach, hasExtendedTimestamp)
			}

			var msgs []*rawmessage.Message

			for {
				msg, err := r.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				msgs = append(msgs, msg)
			}

			require.Equal(t, ca.messages, msgs)
		})
	}
}

func TestReaderAdditional(t *testing.T) {
	for _, ca := range []struct {
		name     string
		messages []*rawmessage.Message
		chunks   []chunk.Chunk
	}{
		{
			"(chunk0) + (chunk3)",
			[]*rawmessage.Message{
				{
					ChunkStreamID:   27,
					Timestamp:       4279383126000000,
					Type:            6,
					MessageStreamID: 3123,
					Body:            bytes.Repeat([]byte{5}, 15),
				},
				{
					ChunkStreamID:   27,
					Timestamp:       4279383126000000,
					Type:            6,
					MessageStreamID: 3123,
					Body:            bytes.Repeat([]byte{6}, 15),
				},
			},
			[]chunk.Chunk{
				&chunk.Chunk0{
					ChunkStreamID:   27,
					Timestamp:       4279383126,
					Type:            6,
					MessageStreamID: 3123,
					BodyLen:         15,
					Body:            bytes.Repeat([]byte{5}, 15),
				},
				&chunk.Chunk3{
					ChunkStreamID:     27,
					ExtendedTimestamp: 4279383126,
					Body:              bytes.Repeat([]byte{6}, 15),
				},
			},
		},
	} { //nolint:dupl
		t.Run(ca.name, func(t *testing.T) {
			var buf bytes.Buffer
			br := bytecounter.NewReader(&buf)
			r := &rawmessage.Reader{
				BR:  br,
				BCR: br,
				OnAckNeeded: func(_ uint32) error {
					return nil
				},
			}
			r.Initialize()

			hasExtendedTimestamp := false

			for _, cach := range ca.chunks {
				buf2, err := cach.Marshal(hasExtendedTimestamp) //nolint:staticcheck
				require.NoError(t, err)
				buf.Write(buf2)
				hasExtendedTimestamp = chunkHasExtendedTimestamp(cach, hasExtendedTimestamp)
			}

			var msgs []*rawmessage.Message

			for {
				msg, err := r.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				msgs = append(msgs, msg)
			}

			require.Equal(t, ca.messages, msgs)
		})
	}
}

func TestReaderAbortChunkStream(t *testing.T) {
	for _, ca := range []struct {
		name    string
		abortID uint32
	}{
		{
			name:    "unsupported low csid",
			abortID: 1,
		},
		{
			name:    "unsupported high csid",
			abortID: 64,
		},
		{
			name:    "known csid",
			abortID: 27,
		},
		{
			name:    "unknown csid",
			abortID: 28,
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			var buf bytes.Buffer
			bc := bytecounter.NewReader(&buf)
			r := &rawmessage.Reader{
				BR:  bc,
				BCR: bc,
				OnAckNeeded: func(_ uint32) error {
					return nil
				},
			}
			r.Initialize()
			_ = r.SetChunkSize(64)

			chunks := []chunk.Chunk{
				&chunk.Chunk0{
					ChunkStreamID:   27,
					Timestamp:       1000,
					Type:            6,
					MessageStreamID: 3123,
					BodyLen:         80,
					Body:            bytes.Repeat([]byte{0xaa}, 64),
				},
				&chunk.Chunk0{
					ChunkStreamID:   28,
					Timestamp:       2000,
					Type:            7,
					MessageStreamID: 4123,
					BodyLen:         4,
					Body:            []byte{1, 2, 3, 4},
				},
				&chunk.Chunk0{
					ChunkStreamID:   27,
					Timestamp:       3000,
					Type:            8,
					MessageStreamID: 3123,
					BodyLen:         3,
					Body:            []byte{9, 8, 7},
				},
			}

			hasExtendedTimestamp := false
			for _, cach := range chunks {
				buf2, err := cach.Marshal(hasExtendedTimestamp) //nolint:staticcheck
				require.NoError(t, err)
				buf.Write(buf2)
				hasExtendedTimestamp = chunkHasExtendedTimestamp(cach, hasExtendedTimestamp)
			}

			msg, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, byte(28), msg.ChunkStreamID)
			require.Equal(t, []byte{1, 2, 3, 4}, msg.Body)

			r.AbortChunkStream(ca.abortID)

			msg, err = r.Read()
			if ca.abortID != 27 {
				require.EqualError(t, err, "received type 0 chunk but expected type 3 chunk")
				return
			}

			require.NoError(t, err)
			require.Equal(t, byte(27), msg.ChunkStreamID)
			require.Equal(t, []byte{9, 8, 7}, msg.Body)
		})
	}
}

func TestReaderAcknowledge(t *testing.T) {
	for _, ca := range []string{"standard", "overflow"} {
		t.Run(ca, func(t *testing.T) {
			onAckCalled := make(chan struct{})

			var buf bytes.Buffer
			bc := bytecounter.NewReader(&buf)
			r := &rawmessage.Reader{
				BR:  bc,
				BCR: bc,
				OnAckNeeded: func(_ uint32) error {
					close(onAckCalled)
					return nil
				},
			}
			r.Initialize()

			if ca == "overflow" {
				bc.SetCount(4294967096)
				r.LastAckCount = 4294967096
			}

			err := r.SetChunkSize(65536)
			require.NoError(t, err)

			r.SetWindowAckSize(100)

			buf2, err := chunk.Chunk0{ //nolint:staticcheck
				ChunkStreamID:   27,
				Timestamp:       18576,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         200,
				Body:            bytes.Repeat([]byte{0x03}, 200),
			}.Marshal(false)
			require.NoError(t, err)
			buf.Write(buf2)

			_, err = r.Read()
			require.NoError(t, err)

			<-onAckCalled
		})
	}
}

func FuzzReader(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte) {
		bcr := bytecounter.NewReader(bytes.NewReader(b))
		r := &rawmessage.Reader{
			BR:  bcr,
			BCR: bcr,
			OnAckNeeded: func(_ uint32) error {
				return nil
			},
		}
		r.Initialize()

		var buf bytes.Buffer
		bcw := bytecounter.NewWriter(&buf)
		w := &rawmessage.Writer{
			BW:               bcw,
			BCW:              bcw,
			CheckAcknowledge: true,
		}
		w.Initialize()

		for {
			msg, err := r.Read()
			if err == nil {
				err = w.Write(msg)
				require.NoError(t, err)
			} else {
				break
			}
		}
	})
}

func TestReaderReset(t *testing.T) {
	// after a reset, the chunk size goes back to the default value and the
	// number of acknowledged bytes is cleared.
	var buf bytes.Buffer
	bc := bytecounter.NewReader(&buf)
	r := &rawmessage.Reader{
		BR:          bc,
		BCR:         bc,
		OnAckNeeded: func(uint32) error { return nil },
	}
	r.Initialize()

	require.NoError(t, r.SetChunkSize(65536))
	r.SetWindowAckSize(1000)
	r.LastAckCount = 500

	r.Reset()

	// a message larger than the default chunk size must be split into chunks.
	w := &rawmessage.Writer{
		BW:               &buf,
		BCW:              bytecounter.NewWriter(&buf),
		CheckAcknowledge: false,
	}
	w.Initialize()

	body := bytes.Repeat([]byte{0x03}, 300)
	require.NoError(t, w.Write(&rawmessage.Message{
		ChunkStreamID: 6,
		Type:          9,
		Body:          body,
	}))

	// the writer above starts from the default chunk size as well.
	msg, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, body, msg.Body)
}

func TestReaderResetDropsPartialMessage(t *testing.T) {
	// encode a 300-byte message with the default chunk size (128): it becomes
	// three chunks (type 0 + type 3 + type 3).
	encode := func(msgs ...*rawmessage.Message) []byte {
		var buf bytes.Buffer
		w := &rawmessage.Writer{
			BW:               &buf,
			BCW:              bytecounter.NewWriter(&buf),
			CheckAcknowledge: false,
		}
		w.Initialize()
		for _, msg := range msgs {
			require.NoError(t, w.Write(msg))
		}
		return buf.Bytes()
	}

	enc := encode(&rawmessage.Message{
		ChunkStreamID: 6,
		Type:          9,
		Body:          bytes.Repeat([]byte{0x05}, 300),
	})

	// keep only the first two chunks, i.e. drop the third one.
	third := bytes.LastIndexByte(enc, 0xC0|6)
	require.Greater(t, third, 0)
	partial := enc[:third]

	next := encode(&rawmessage.Message{
		ChunkStreamID: 6,
		Type:          9,
		Body:          []byte{0x0A, 0x0B, 0x0C},
	})

	src := &switchReader{}
	src.set(partial)

	r := &rawmessage.Reader{
		BR:          src,
		BCR:         bytecounter.NewReader(src),
		OnAckNeeded: func(uint32) error { return nil },
	}
	r.Initialize()

	// the partial message cannot be completed: the reader reaches the end of
	// the source while a message is being assembled.
	_, err := r.Read()
	require.Error(t, err)

	// a reset must drop the assembly state, so that a fresh type 0 chunk on the
	// same chunk stream is accepted instead of being refused as "expected type
	// 3 chunk".
	r.Reset()
	src.set(next)

	msg, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, []byte{0x0A, 0x0B, 0x0C}, msg.Body)
}

// switchReader is a byte source whose content can be replaced.
type switchReader struct {
	data []byte
}

func (s *switchReader) set(data []byte) {
	s.data = data
}

func (s *switchReader) Read(p []byte) (int, error) {
	if len(s.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.data)
	s.data = s.data[n:]
	return n, nil
}

func TestWriterReset(t *testing.T) {
	var buf bytes.Buffer
	bc := bytecounter.NewWriter(&buf)
	w := &rawmessage.Writer{BW: bc, BCW: bc, CheckAcknowledge: true}
	w.Initialize()

	w.SetChunkSize(65536)
	w.SetWindowAckSize(100)
	w.SetAcknowledgeValue(1234)

	w.Reset()

	// after the reset, a message larger than 128 bytes must be split into
	// chunks, and the acknowledge window must be disabled again.
	body := bytes.Repeat([]byte{0x07}, 300)
	require.NoError(t, w.Write(&rawmessage.Message{
		ChunkStreamID: 6,
		Type:          9,
		Body:          body,
	}))

	bc.SetCount(4294967096)

	// with the window cleared, no acknowledge is expected.
	require.NoError(t, w.Write(&rawmessage.Message{
		ChunkStreamID: 6,
		Type:          9,
		Body:          body,
	}))
}
