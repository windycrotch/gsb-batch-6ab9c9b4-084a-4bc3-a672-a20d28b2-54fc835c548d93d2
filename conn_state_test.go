package gortmplib_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/amf0"
	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/handshake"
	"github.com/bluenviron/gortmplib/pkg/message"
	"github.com/bluenviron/gortmplib/pkg/rawmessage"
	"github.com/bluenviron/gortmplib/pkg/rc4rw"
)

func testConnectCommand() *message.CommandAMF0 {
	return &message.CommandAMF0{
		ChunkStreamID: 3,
		Name:          "connect",
		CommandID:     1,
		Arguments: []any{
			amf0.Object{
				{Key: "app", Value: "stream"},
				{Key: "flashVer", Value: "LNX 9,0,124,2"},
				{Key: "tcUrl", Value: "rtmp://127.0.0.1:9121/stream"},
			},
		},
	}
}

// clientSide performs a client-side handshake and sends the connect command,
// returning the message read/writer that must be used afterwards.
func clientSide(t *testing.T, clientEnd net.Conn, encrypted bool) *message.ReadWriter {
	t.Helper()

	bc := bytecounter.NewReadWriter(clientEnd)

	keyIn, keyOut, err := handshake.DoClient(bc, encrypted, false)
	require.NoError(t, err)

	var rw io.ReadWriter = bc
	if keyIn != nil {
		rw, err = rc4rw.New(bc, keyIn, keyOut)
		require.NoError(t, err)
	}

	mrw := message.NewReadWriter(rw, bc, false)
	require.NoError(t, mrw.Write(testConnectCommand()))

	return mrw
}

func startServer(sc *gortmplib.ServerConn) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- sc.Initialize()
	}()
	return done
}

func TestConnStatePlainHandshake(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close() //nolint:errcheck
	defer clientEnd.Close() //nolint:errcheck

	sc := &gortmplib.ServerConn{RW: serverEnd}
	require.False(t, sc.Handshaked())

	done := startServer(sc)
	mrw := clientSide(t, clientEnd, false)

	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())

	// after a plain handshake messages are exchanged in plaintext.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- mrw.Write(&message.SetChunkSize{Value: 4096})
	}()

	msg, err := sc.Read()
	require.NoError(t, err)
	require.Equal(t, &message.SetChunkSize{Value: 4096}, msg)
	require.NoError(t, <-writeDone)
}

func TestConnStateEncryptedHandshake(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close() //nolint:errcheck
	defer clientEnd.Close() //nolint:errcheck

	sc := &gortmplib.ServerConn{RW: serverEnd}
	require.False(t, sc.Handshaked())

	done := startServer(sc)
	mrw := clientSide(t, clientEnd, true)

	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())

	// after an encrypted handshake messages travel through RC4.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- mrw.Write(&message.SetChunkSize{Value: 4096})
	}()

	msg, err := sc.Read()
	require.NoError(t, err)
	require.Equal(t, &message.SetChunkSize{Value: 4096}, msg)
	require.NoError(t, <-writeDone)
}

func TestConnStateEncryptedKeysDirection(t *testing.T) {
	for range 3 {
		clientEnd, serverEnd := net.Pipe()

		type result struct {
			in  []byte
			out []byte
			err error
		}
		clientRes := make(chan result, 1)

		go func() {
			in, out, err := handshake.DoClient(bytecounter.NewReadWriter(clientEnd), true, false)
			clientRes <- result{in, out, err}
		}()

		inKey, outKey, err := handshake.DoServer(bytecounter.NewReadWriter(serverEnd), false)
		require.NoError(t, err)

		res := <-clientRes
		require.NoError(t, res.err)

		// the client encryption key must equal the server decryption key and
		// vice versa.
		require.Equal(t, inKey, res.out)
		require.Equal(t, outKey, res.in)

		clientEnd.Close() //nolint:errcheck
		serverEnd.Close() //nolint:errcheck
	}
}

func TestConnStateHandshakeFailureThenRetry(t *testing.T) {
	// a successful encrypted handshake leaves keys on the connection object.
	serverEnd1, clientEnd1 := net.Pipe()

	sc := &gortmplib.ServerConn{RW: serverEnd1}

	done := startServer(sc)
	_ = clientSide(t, clientEnd1, true)
	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())

	clientEnd1.Close() //nolint:errcheck
	serverEnd1.Close() //nolint:errcheck

	// a failed attempt must reset the connection rather than leaving the old
	// keys in place.
	sc.RW = &failingConn{}
	require.Error(t, sc.Initialize())
	require.False(t, sc.Handshaked())

	require.ErrorIs(t, sc.Write(&message.SetChunkSize{Value: 128}), gortmplib.ErrNotHandshaked)
	_, err := sc.Read()
	require.ErrorIs(t, err, gortmplib.ErrNotHandshaked)

	// a plain handshake on the very same connection object must now succeed.
	// If the old RC4 keys had survived, the plaintext connect command would be
	// decoded as garbage and this would fail.
	serverEnd2, clientEnd2 := net.Pipe()
	defer serverEnd2.Close() //nolint:errcheck
	defer clientEnd2.Close() //nolint:errcheck

	sc.RW = serverEnd2

	done = startServer(sc)
	_ = clientSide(t, clientEnd2, false)
	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())
}

func TestConnStateReadWriteRequireHandshake(t *testing.T) {
	sc := &gortmplib.ServerConn{RW: &failingConn{}}

	require.False(t, sc.Handshaked())
	require.ErrorIs(t, sc.Write(&message.SetChunkSize{Value: 128}), gortmplib.ErrNotHandshaked)
	_, err := sc.Read()
	require.ErrorIs(t, err, gortmplib.ErrNotHandshaked)
}

func TestConnStateReadErrorResetsHandshake(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()

	sc := &gortmplib.ServerConn{RW: serverEnd}

	done := startServer(sc)
	_ = clientSide(t, clientEnd, false)
	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())

	// the peer disconnects: the next read fails and the connection must go back
	// to the not handshaked state.
	clientEnd.Close() //nolint:errcheck

	_, err := sc.Read()
	require.Error(t, err)
	require.False(t, sc.Handshaked())

	serverEnd.Close() //nolint:errcheck
}

// failingConn is a connection whose read side is already closed.
type failingConn struct{}

func (failingConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (failingConn) Write(p []byte) (int, error) { return len(p), nil }

// fixedRW is a write-only ReadWriter.
type fixedRW struct {
	w io.Writer
}

func (f *fixedRW) Read([]byte) (int, error) { return 0, io.EOF }
func (f *fixedRW) Write(p []byte) (int, error) {
	return f.w.Write(p)
}

// replayConn is an in-memory connection whose incoming bytes can be replaced
// between handshake attempts.
type replayConn struct {
	mutex  sync.Mutex
	input  []byte
	output bytes.Buffer
	closed bool
}

func (c *replayConn) setInput(b []byte) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.input = b
}

func (c *replayConn) closeInput() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.closed = true
}

func (c *replayConn) Read(p []byte) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed || len(c.input) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.input)
	c.input = c.input[n:]
	return n, nil
}

func (c *replayConn) Write(p []byte) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return c.output.Write(p)
}

// TestConnStateReuseAfterFailedEncryptedHandshake reuses the same connection
// object after a failed encrypted handshake and checks that the stale keys do
// not leak into the next attempt.
func TestConnStateReuseAfterFailedEncryptedHandshake(t *testing.T) {
	// the first attempt is an encrypted handshake that the client abandons.
	conn := &replayConn{}

	var c0buf bytes.Buffer
	require.NoError(t, handshake.C0S0{Version: 6}.Write(&c0buf))
	conn.setInput(c0buf.Bytes())
	conn.closeInput()

	sc := &gortmplib.ServerConn{RW: conn}
	require.Error(t, sc.Initialize())
	require.False(t, sc.Handshaked())

	// the very same ServerConn object is then reused with a plaintext
	// handshake: if stale RC4 keys or chunk state survived, the connect command
	// would be misdecoded and this would fail.
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close() //nolint:errcheck
	defer clientEnd.Close() //nolint:errcheck

	sc.RW = serverEnd

	done := startServer(sc)
	_ = clientSide(t, clientEnd, false)
	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())
}

// TestConnStateResetOnSameConnection checks that Reset allows a brand new
// handshake on the same connection object, with the chunk layer back at its
// defaults.
func TestConnStateResetOnSameConnection(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close() //nolint:errcheck
	defer clientEnd.Close() //nolint:errcheck

	sc := &gortmplib.ServerConn{RW: serverEnd}

	done := startServer(sc)
	clientMRW := clientSide(t, clientEnd, false)
	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())

	// grow the chunk size, as a client would do.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- clientMRW.Write(&message.SetChunkSize{Value: 65536})
	}()
	_, err := sc.Read()
	require.NoError(t, err)
	require.NoError(t, <-writeDone)

	sc.Reset()
	require.False(t, sc.Handshaked())
	require.ErrorIs(t, sc.Write(&message.SetChunkSize{Value: 128}), gortmplib.ErrNotHandshaked)

	// a new handshake on the same connection succeeds; the client resets its
	// own chunk size accordingly.
	done = startServer(sc)
	clientMRW = clientSide(t, clientEnd, false)
	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())

	// the server is back at the default chunk size: a body larger than 128
	// bytes must be split into several chunks. To prove it, send a 400-byte
	// message and decode it with a fresh raw reader that uses the default chunk
	// size.
	writeDone = make(chan error, 1)
	go func() {
		writeDone <- clientMRW.Write(&message.Video{
			ChunkStreamID:   message.VideoChunkStreamID,
			MessageStreamID: 1,
			Codec:           message.CodecH264,
			FrameType:       message.VideoFrameTypeKeyFrame,
			PacketType:      message.VideoPacketTypeAU,
			AU:              bytes.Repeat([]byte{0x01}, 400),
		})
	}()

	msg, err := sc.Read()
	require.NoError(t, err)
	require.NoError(t, <-writeDone)
	require.IsType(t, &message.Video{}, msg)
}

// TestConnStateConcurrentWriteResetWholeMessages races message writes against
// resets and checks that every successful write is delivered as a whole
// message, never as a truncated one.
func TestConnStateConcurrentWriteResetWholeMessages(t *testing.T) {
	const totalWrites = 100
	const bodyLen = 8192

	for range 5 {
		serverEnd, clientEnd := net.Pipe()

		sc := &gortmplib.ServerConn{RW: serverEnd}

		done := startServer(sc)
		clientMRW := clientSide(t, clientEnd, false)
		require.NoError(t, <-done)
		require.True(t, sc.Handshaked())

		// the client keeps reading, so that writes do not block.
		type drainResult struct {
			received int
			partial  bool
			err      error
		}
		drainRes := make(chan drainResult, 1)

		go func() {
			var res drainResult
			for res.received < totalWrites {
				msg, err := clientMRW.Read()
				if err != nil {
					res.err = err
					break
				}

				if v, ok := msg.(*message.Video); ok {
					if len(v.AU) != bodyLen {
						res.partial = true
					}
					res.received++
				}
			}
			drainRes <- res
		}()

		var successes atomic.Int64
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()

			for range totalWrites {
				err := sc.Write(&message.Video{
					ChunkStreamID:   message.VideoChunkStreamID,
					MessageStreamID: 1,
					Codec:           message.CodecH264,
					FrameType:       message.VideoFrameTypeKeyFrame,
					PacketType:      message.VideoPacketTypeAU,
					AU:              bytes.Repeat([]byte{0x04}, bodyLen),
				})
				if err == nil {
					successes.Add(1)
				} else {
					require.True(t, errors.Is(err, gortmplib.ErrNotHandshaked) ||
						errors.Is(err, gortmplib.ErrConnClosed), "unexpected error: %v", err)
				}
			}
		}()

		go func() {
			defer wg.Done()
			for range totalWrites {
				sc.Reset()
			}
		}()

		wg.Wait()

		// let the client consume everything that was written.
		serverEnd.Close() //nolint:errcheck
		clientEnd.Close() //nolint:errcheck

		res := <-drainRes

		require.False(t, res.partial, "a truncated message was delivered")
		require.Equal(t, int(successes.Load()), res.received,
			"every successful write must correspond to one whole received message")
	}
}

// TestConnStateRetrySameTransport performs a failed handshake followed by a
// successful one on the very same underlying transport. The first attempt
// consumes one invalid byte (an unsupported C0 version) and must reset the
// connection; the second attempt reads the valid handshake that follows in the
// same byte stream.
func TestConnStateRetrySameTransport(t *testing.T) {
	// client -> server bytes of a valid plaintext handshake followed by the
	// connect command.
	var stream bytes.Buffer
	stream.WriteByte(0x03)                         // C0
	stream.Write(bytes.Repeat([]byte{0x11}, 1536)) // C1
	stream.Write(bytes.Repeat([]byte{0x22}, 1536)) // C2
	var cmdBuf bytes.Buffer
	cmdBC := bytecounter.NewReadWriter(&fixedRW{w: &cmdBuf})
	require.NoError(t, message.NewReadWriter(cmdBC, cmdBC, false).Write(testConnectCommand()))
	stream.Write(cmdBuf.Bytes())

	// preface with an invalid C0 (version 9), which makes the first attempt
	// fail after consuming exactly one byte.
	input := append([]byte{0x09}, stream.Bytes()...)

	conn := &replayConn{}
	conn.setInput(input)

	sc := &gortmplib.ServerConn{RW: conn}

	require.Error(t, sc.Initialize())
	require.False(t, sc.Handshaked())

	// the second attempt continues from the same transport.
	require.NoError(t, sc.Initialize())
	require.True(t, sc.Handshaked())
}

// TestConnStateResetDiscardsEncryptedKeys resets the connection after a
// successful encrypted handshake and verifies that the next handshake on the
// same transport is plaintext: if the old RC4 ciphers survived, the plaintext
// connect command would be decoded as garbage.
func TestConnStateResetDiscardsEncryptedKeys(t *testing.T) {
	// build the client side of an encrypted handshake followed by a connect
	// command, using a recording duplex.
	var script bytes.Buffer

	// C0 (version 6)
	script.WriteByte(0x06)
	// C1: 1536 bytes. The digest and public key are irrelevant for this test,
	// because the server validates them; a random C1 is enough to exercise the
	// reset path (the handshake itself is expected to fail).
	script.Write(bytes.Repeat([]byte{0x33}, 1536))

	conn := &replayConn{}
	conn.setInput(script.Bytes())
	conn.closeInput()

	sc := &gortmplib.ServerConn{RW: conn}

	// the encrypted handshake fails (C1 is invalid), which must reset the
	// connection.
	require.Error(t, sc.Initialize())
	require.False(t, sc.Handshaked())

	// a plaintext handshake on the same object must work.
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close() //nolint:errcheck
	defer clientEnd.Close() //nolint:errcheck

	sc.RW = serverEnd

	done := startServer(sc)
	_ = clientSide(t, clientEnd, false)
	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())
}

// notifyConn signals after each successful Read, so tests can observe when a
// message has been partially consumed.
type notifyConn struct {
	io.ReadWriter
	onRead func()
}

func (c *notifyConn) Read(p []byte) (int, error) {
	n, err := c.ReadWriter.Read(p)
	if n > 0 && c.onRead != nil {
		c.onRead()
	}
	return n, err
}

func TestConnStateResetWhileAssemblingMessage(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close() //nolint:errcheck
	defer clientEnd.Close() //nolint:errcheck

	readOccurred := make(chan struct{}, 16)
	sc := &gortmplib.ServerConn{
		RW: &notifyConn{
			ReadWriter: serverEnd,
			onRead: func() {
				select {
				case readOccurred <- struct{}{}:
				default:
				}
			},
		},
	}

	done := startServer(sc)
	clientMRW := clientSide(t, clientEnd, false)
	require.NoError(t, <-done)
	require.True(t, sc.Handshaked())

	// ask the server to accept 128-byte chunks.
	chunkDone := make(chan error, 1)
	go func() {
		chunkDone <- clientMRW.Write(&message.SetChunkSize{Value: 128})
	}()
	_, err := sc.Read()
	require.NoError(t, err)
	require.NoError(t, <-chunkDone)

	// encode a 300-byte message as three chunks and split it after the second.
	var buf bytes.Buffer
	w := &rawmessage.Writer{BW: &buf, BCW: bytecounter.NewWriter(&buf)}
	w.Initialize()
	w.SetChunkSize(128)
	// a valid AVC video message body: 0x17 = key frame + AVC, 0x01 = raw frame.
	videoBody := append([]byte{0x17, 0x01, 0x00, 0x00, 0x00}, bytes.Repeat([]byte{0x06}, 295)...)
	require.NoError(t, w.Write(&rawmessage.Message{
		ChunkStreamID:   message.VideoChunkStreamID,
		Type:            uint8(message.TypeVideo),
		MessageStreamID: 1,
		Body:            videoBody,
	}))
	enc := buf.Bytes()
	third := bytes.LastIndexByte(enc, 0xC0|message.VideoChunkStreamID)
	require.Greater(t, third, 0)

	// start a read on the server and feed it only the first two chunks.
	readResult := make(chan error, 1)
	go func() {
		_, err2 := sc.Read()
		readResult <- err2
	}()

	firstWrite := make(chan error, 1)
	go func() {
		_, err2 := clientEnd.Write(enc[:third])
		firstWrite <- err2
	}()

	// wait until the server consumed the partial message.
	<-readOccurred
	require.NoError(t, <-firstWrite)

	// reset the connection while the message is being assembled.
	sc.Reset()
	require.False(t, sc.Handshaked())

	// deliver the last chunk: the in-flight read must discard the assembled
	// message rather than returning it.
	lastWrite := make(chan error, 1)
	go func() {
		_, err2 := clientEnd.Write(enc[third:])
		lastWrite <- err2
	}()

	require.ErrorIs(t, <-readResult, gortmplib.ErrNotHandshaked)
	require.NoError(t, <-lastWrite)

	// the connection stays not handshaked.
	_, err = sc.Read()
	require.ErrorIs(t, err, gortmplib.ErrNotHandshaked)
}

// TestConnStateConcurrentCloseAndWrite checks that closing the connection while
// a write is in progress does not deadlock and does not report a partial write
// as successful.
func TestConnStateConcurrentCloseAndWrite(t *testing.T) {
	for range 50 {
		serverEnd, clientEnd := net.Pipe()

		sc := &gortmplib.ServerConn{RW: serverEnd}
		done := startServer(sc)
		_ = clientSide(t, clientEnd, false)
		require.NoError(t, <-done)

		writeErr := make(chan error, 1)

		go func() {
			writeErr <- sc.Write(&message.Video{
				ChunkStreamID:   message.VideoChunkStreamID,
				MessageStreamID: 1,
				Codec:           message.CodecH264,
				FrameType:       message.VideoFrameTypeKeyFrame,
				PacketType:      message.VideoPacketTypeAU,
				AU:              bytes.Repeat([]byte{0x08}, 8192),
			})
		}()

		sc.Close() //nolint:errcheck

		err := <-writeErr
		if err != nil {
			require.True(t, errors.Is(err, gortmplib.ErrConnClosed) ||
				errors.Is(err, io.ErrClosedPipe), "unexpected error: %v", err)
		}

		require.False(t, sc.Handshaked())

		clientEnd.Close() //nolint:errcheck
	}
}

// TestClientResetBeforeInitialize checks that a Client that never completed the
// handshake refuses reads and writes.
func TestClientResetBeforeInitialize(t *testing.T) {
	c := &gortmplib.Client{}
	require.False(t, c.Handshaked())

	require.ErrorIs(t, c.Write(&message.SetChunkSize{Value: 128}), gortmplib.ErrNotHandshaked)
	_, err := c.Read()
	require.ErrorIs(t, err, gortmplib.ErrNotHandshaked)

	c.Reset()
	require.False(t, c.Handshaked())
}

// TestClientHandshakeLifecycle drives a full publish flow: the client and the
// server negotiate a handshake (plaintext or encrypted) and the client reaches
// the handshaked state with the keys oriented correctly.
func TestClientHandshakeLifecycle(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "encrypted"}[encrypted], func(t *testing.T) {
			clientEnd, serverEnd := net.Pipe()

			serverDone := make(chan error, 1)

			go func() {
				sc := &gortmplib.ServerConn{RW: serverEnd}

				// the server-side handshake is always performed in plaintext by
				// ServerConn; RTMPE is negotiated when the client uses version 6
				// in C0, which the client does when Encrypted is set.
				err := sc.Initialize()
				if err != nil {
					serverDone <- err
					return
				}

				err = sc.AcceptConn()
				if err != nil {
					serverDone <- err
					return
				}

				serverDone <- sc.AcceptAction()
			}()

			u, err := url.Parse("rtmp://127.0.0.1:1935/stream")
			require.NoError(t, err)

			c := &gortmplib.Client{
				URL:       u,
				Publish:   true,
				Encrypted: encrypted,
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return clientEnd, nil
				},
			}

			require.NoError(t, c.Initialize(context.Background()))
			require.True(t, c.Handshaked())
			require.NoError(t, <-serverDone)

			c.Close()         //nolint:errcheck
			serverEnd.Close() //nolint:errcheck
		})
	}
}
