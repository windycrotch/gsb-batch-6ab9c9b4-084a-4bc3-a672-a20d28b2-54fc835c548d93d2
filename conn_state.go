package gortmplib

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/message"
	"github.com/bluenviron/gortmplib/pkg/rc4rw"
)

var (
	// ErrNotHandshaked is returned when reading or writing before the handshake
	// has been completed, and after the connection has been reset.
	ErrNotHandshaked = errors.New("handshake not completed")

	// ErrConnClosed is returned when reading or writing on a closed connection.
	ErrConnClosed = errors.New("connection closed")
)

const (
	rc4KeyLength = 16
)

// transport wraps the underlying ReadWriter and gives access to its Close
// method, if any. It is stored in an atomic pointer so that Close() can
// interrupt a blocked Read or Write without taking the connection mutex.
type transport struct {
	io.ReadWriter
}

func (t *transport) Close() error {
	if closer, ok := t.ReadWriter.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// connState is the connection lifecycle shared by the handshake and by the
// chunk (message) read/write layer.
//
// A connection is in one of three states:
//
//   - not handshaked: reads and writes are refused with ErrNotHandshaked;
//   - handshaked: reads and writes go through the chunk layer;
//   - closed: reads and writes are refused with ErrConnClosed.
//
// Every handshake attempt begins from a clean state: the RC4 ciphers of the
// previous attempt, the per-chunk-stream state, the chunk size, the window
// acknowledgement size and the last received acknowledgement are all
// discarded. Writes are serialized with resets, so that a message is either
// written entirely and reported as successful, or the write fails and the
// connection goes back to the not handshaked state.
type connState struct {
	mutex     sync.Mutex
	readMutex sync.Mutex

	tr    atomic.Pointer[transport]
	bcPtr atomic.Pointer[bytecounter.ReadWriter]

	bc     *bytecounter.ReadWriter
	rwData io.ReadWriter
	mrw    *message.ReadWriter

	handshaked bool
	closed     atomic.Bool
	gen        uint64
}

// begin binds a new underlying transport, discards any previous handshake
// state and starts a new handshake attempt. It returns the generation of the
// attempt and the byte counter that must be used to exchange handshake bytes.
func (s *connState) begin(rw io.ReadWriter) (uint64, *bytecounter.ReadWriter, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if rw == nil {
		return 0, nil, errors.New("nil connection")
	}

	s.tr.Store(&transport{rw})
	s.bc = bytecounter.NewReadWriter(rw)
	s.bcPtr.Store(s.bc)

	s.closed.Store(false)
	s.rwData = s.bc
	s.mrw = nil
	s.handshaked = false
	s.gen++

	return s.gen, s.bc, nil
}

// finishHandshake finalizes a successful handshake attempt. When keys are
// provided, the connection switches to RC4 encryption: the two ciphers are
// created exactly once here, so their initial discard happens exactly once per
// direction. The keys must already be oriented towards this side of the
// connection (keyIn decrypts received bytes, keyOut encrypts sent bytes).
func (s *connState) finishHandshake(gen uint64, keyIn, keyOut []byte, checkAcknowledge bool) error {
	if (keyIn == nil) != (keyOut == nil) ||
		(keyIn != nil && (len(keyIn) != rc4KeyLength || len(keyOut) != rc4KeyLength)) {
		s.failHandshake(gen)
		return errors.New("invalid encryption keys")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.gen != gen || s.closed.Load() || s.bc == nil {
		return ErrNotHandshaked
	}

	rw := s.rwData
	if keyIn != nil {
		var err error
		rw, err = rc4rw.New(s.bc, keyIn, keyOut)
		if err != nil {
			s.resetLocked()
			return err
		}
	}

	s.rwData = rw
	s.mrw = message.NewReadWriter(rw, s.bc, checkAcknowledge)
	s.handshaked = true

	return nil
}

// failHandshake resets the connection after a failed handshake attempt. It is
// a no-op when a newer attempt already replaced this one.
func (s *connState) failHandshake(gen uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.gen == gen && !s.closed.Load() {
		s.resetLocked()
	}
}

// resetLocked resets the connection to the not handshaked state. It must be
// called with the mutex held.
func (s *connState) resetLocked() {
	s.handshaked = false
	s.rwData = nil
	s.mrw = nil
	s.gen++
}

// Reset resets the connection lifecycle to the not handshaked state, discarding
// the RC4 ciphers, the chunk layer state and any partially assembled message.
// The underlying transport is left open and can be used for a new handshake.
//
// It is safe to call it concurrently with Read and Write. It does not
// interrupt a Read that is already blocked on the transport; call Close for
// that. It does not reopen a transport that has already been closed.
func (s *connState) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.resetLocked()
}

// close closes the underlying transport and resets the connection. Closing the
// transport before taking the mutex allows it to interrupt a blocked Read or
// Write.
func (s *connState) close() error {
	if s.closed.Swap(true) {
		return nil
	}

	var err error
	if tr := s.tr.Load(); tr != nil {
		err = tr.Close()
	}

	s.mutex.Lock()
	s.resetLocked()
	s.mutex.Unlock()

	return err
}

// Handshaked reports whether the handshake has been completed successfully and
// the connection has not been reset or closed since then.
func (s *connState) Handshaked() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.handshaked && !s.closed.Load()
}

// Read reads a message. A message whose assembly is interrupted by a reset is
// discarded instead of being returned.
func (s *connState) Read() (message.Message, error) {
	s.readMutex.Lock()
	defer s.readMutex.Unlock()

	s.mutex.Lock()
	if s.closed.Load() {
		s.mutex.Unlock()
		return nil, ErrConnClosed
	}
	if !s.handshaked || s.mrw == nil {
		s.mutex.Unlock()
		return nil, ErrNotHandshaked
	}
	mrw := s.mrw
	gen := s.gen
	s.mutex.Unlock()

	msg, err := mrw.Read()

	s.mutex.Lock()
	stale := s.gen != gen
	if err != nil && !stale && !s.closed.Load() {
		// a read error invalidates the handshake.
		s.resetLocked()
	}
	s.mutex.Unlock()

	if err != nil {
		return nil, err
	}

	if stale {
		// the connection was reset while the message was being assembled.
		return nil, ErrNotHandshaked
	}

	return msg, nil
}

// Write writes a message. The whole message is written while holding the state
// mutex, so a concurrent reset or close either happens before the write (and
// the write fails) or after it (and the write has been performed entirely).
func (s *connState) Write(msg message.Message) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed.Load() {
		return ErrConnClosed
	}
	if !s.handshaked || s.mrw == nil {
		return ErrNotHandshaked
	}

	gen := s.gen
	err := s.mrw.Write(msg)
	if err != nil && s.gen == gen && !s.closed.Load() {
		// a write error invalidates the handshake.
		s.resetLocked()
	}

	return err
}

func (s *connState) readCommand() (*message.CommandAMF0, error) {
	for {
		msg, err := s.Read()
		if err != nil {
			return nil, err
		}

		if cmd, ok := msg.(*message.CommandAMF0); ok {
			return cmd, nil
		}
	}
}

func (s *connState) readCommandResult(commandID int) (*message.CommandAMF0, error) {
	for {
		msg, err := s.Read()
		if err != nil {
			return nil, err
		}

		if cmd, ok := msg.(*message.CommandAMF0); ok {
			if cmd.CommandID == commandID || (cmd.CommandID == 0 &&
				(cmd.Name == "_result" || cmd.Name == "_error")) {
				return cmd, nil
			}
		}
	}
}

func (s *connState) waitOnStatus(commandID int) (*message.CommandAMF0, error) {
	for {
		msg, err := s.Read()
		if err != nil {
			return nil, err
		}

		if cmd, ok := msg.(*message.CommandAMF0); ok {
			if cmd.CommandID == commandID || (cmd.CommandID == 0 &&
				cmd.Name == "onStatus") {
				return cmd, nil
			}
		}
	}
}

func (s *connState) bytesReceived() uint64 {
	if bc := s.bcPtr.Load(); bc != nil {
		return bc.Reader.Count()
	}
	return 0
}

func (s *connState) bytesSent() uint64 {
	if bc := s.bcPtr.Load(); bc != nil {
		return bc.Writer.Count()
	}
	return 0
}
