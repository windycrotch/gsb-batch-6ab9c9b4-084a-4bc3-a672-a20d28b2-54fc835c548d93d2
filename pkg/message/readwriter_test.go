package message_test

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/message"
)

type duplexRW struct {
	io.Reader
	io.Writer
}

func (d *duplexRW) Read(p []byte) (int, error) {
	return d.Reader.Read(p)
}

func (d *duplexRW) Write(p []byte) (int, error) {
	return d.Writer.Write(p)
}

func TestReadWriterAcknowledge(t *testing.T) {
	var buf1 bytes.Buffer
	var buf2 bytes.Buffer

	bc1 := bytecounter.NewReadWriter(&duplexRW{
		Reader: &buf2,
		Writer: &buf1,
	})
	rw1 := message.NewReadWriter(bc1, bc1, true)
	err := rw1.Write(&message.Acknowledge{
		Value: 7863534,
	})
	require.NoError(t, err)

	bc2 := bytecounter.NewReadWriter(&duplexRW{
		Reader: &buf1,
		Writer: &buf2,
	})
	rw2 := message.NewReadWriter(bc2, bc2, true)
	_, err = rw2.Read()
	require.NoError(t, err)
}

func TestReadWriterPing(t *testing.T) {
	var buf1 bytes.Buffer
	var buf2 bytes.Buffer

	bc1 := bytecounter.NewReadWriter(&duplexRW{
		Reader: &buf2,
		Writer: &buf1,
	})
	rw1 := message.NewReadWriter(bc1, bc1, true)
	err := rw1.Write(&message.UserControlPingRequest{
		ServerTime: 143424312,
	})
	require.NoError(t, err)

	bc2 := bytecounter.NewReadWriter(&duplexRW{
		Reader: &buf1,
		Writer: &buf2,
	})
	rw2 := message.NewReadWriter(bc2, bc2, true)
	_, err = rw2.Read()
	require.NoError(t, err)

	msg, err := rw1.Read()
	require.NoError(t, err)
	require.Equal(t, &message.UserControlPingResponse{
		ServerTime: 143424312,
	}, msg)
}

func TestReadWriterReset(t *testing.T) {
	var buf bytes.Buffer
	bc := bytecounter.NewReadWriter(&duplexRW{
		Reader: &buf,
		Writer: &buf,
	})
	rw := message.NewReadWriter(bc, bc, false)

	require.NoError(t, rw.Write(&message.SetChunkSize{Value: 65536}))
	require.NoError(t, rw.Write(&message.SetWindowAckSize{Value: 2500000}))

	rw.Reset()

	// after the reset the chunk size is the default one again: a 400-byte video
	// message becomes several chunks, so the encoded size is larger than the
	// body plus a single header.
	buf.Reset()
	require.NoError(t, rw.Write(&message.Video{
		ChunkStreamID:   message.VideoChunkStreamID,
		MessageStreamID: 1,
		Codec:           message.CodecH264,
		FrameType:       message.VideoFrameTypeKeyFrame,
		PacketType:      message.VideoPacketTypeAU,
		AU:              bytes.Repeat([]byte{0x01}, 400),
	}))

	// 400 bytes with 128-byte chunks: 4 chunks, each with at least a 1-byte
	// header, so more than 400+4 bytes are written.
	require.Greater(t, buf.Len(), 404)
}

func TestReadWriterConcurrentWriteAndReset(t *testing.T) {
	// a goroutine writes large messages while another resets the chunk layer.
	// The serialized access must keep every written message whole: the byte
	// stream is decoded afterwards and must contain only complete messages.
	const attempts = 100

	var buf bytes.Buffer
	var bufMutex sync.Mutex
	bc := bytecounter.NewReadWriter(&duplexRW{
		Reader: bytes.NewReader(nil),
		Writer: &lockedWriter{mutex: &bufMutex, buf: &buf},
	})
	rw := message.NewReadWriter(bc, bc, false)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range attempts {
			err := rw.Write(&message.Video{
				ChunkStreamID:   message.VideoChunkStreamID,
				MessageStreamID: 1,
				Codec:           message.CodecH264,
				FrameType:       message.VideoFrameTypeKeyFrame,
				PacketType:      message.VideoPacketTypeAU,
				AU:              bytes.Repeat([]byte{0x02}, 4096),
			})
			require.NoError(t, err)
		}
	}()

	go func() {
		defer wg.Done()
		for range attempts {
			rw.Reset()
		}
	}()

	wg.Wait()

	// decoding the written stream with the default chunk size must not fail in
	// the middle of a message and must reconstruct 4096-byte bodies.
	bufMutex.Lock()
	enc := append([]byte(nil), buf.Bytes()...)
	bufMutex.Unlock()

	dec := message.NewReader(
		bytecounter.NewReader(bytes.NewReader(enc)),
		bytecounter.NewReader(bytes.NewReader(enc)),
		nil,
	)

	for {
		msg, err := dec.Read()
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}

		video, ok := msg.(*message.Video)
		require.True(t, ok)
		require.Len(t, video.AU, 4096)
	}
}

type lockedWriter struct {
	mutex *sync.Mutex
	buf   *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.buf.Write(p)
}
