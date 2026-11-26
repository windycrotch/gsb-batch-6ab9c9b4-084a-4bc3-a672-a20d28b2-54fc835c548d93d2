package message

import (
	"io"
	"sync"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/rawmessage"
)

// Writer is a message writer.
// It also drains incoming messages.
type Writer struct {
	BW               io.Writer
	BCW              *bytecounter.Writer
	CheckAcknowledge bool

	mutex sync.Mutex
	rmw   *rawmessage.Writer
}

// Initialize initializes the Writer.
func (w *Writer) Initialize() {
	w.rmw = &rawmessage.Writer{
		BW:               w.BW,
		BCW:              w.BCW,
		CheckAcknowledge: w.CheckAcknowledge,
	}
	w.rmw.Initialize()
}

// Reset resets the Writer to its initial state: the maximum chunk size goes
// back to the default value, the window acknowledgement size and the last
// received acknowledge are cleared and the per-chunk-stream state is discarded.
func (w *Writer) Reset() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.rmw.Reset()
}

// NewWriter allocates a Writer.
//
// Deprecated: use Initialize() instead.
func NewWriter(
	bw io.Writer,
	bcw *bytecounter.Writer,
	checkAcknowledge bool,
) *Writer {
	w := &Writer{
		BW:               bw,
		BCW:              bcw,
		CheckAcknowledge: checkAcknowledge,
	}
	w.Initialize()
	return w
}

// SetAcknowledgeValue sets the value of the last received acknowledge.
func (w *Writer) SetAcknowledgeValue(v uint32) {
	w.rmw.SetAcknowledgeValue(v)
}

// Write writes a message. It is safe to call it concurrently.
//
// The message body is always written entirely: an error means that the
// connection is unusable and that the peers must not accept the message.
func (w *Writer) Write(msg Message) error {
	// this is necessary to synchronize rmw.Write with SetChunkSize and SetWindowAckSize.
	w.mutex.Lock()
	defer w.mutex.Unlock()

	raw, err := msg.marshal()
	if err != nil {
		return err
	}

	err = w.rmw.Write(raw)
	if err != nil {
		return err
	}

	switch tmsg := msg.(type) {
	case *SetChunkSize:
		w.rmw.SetChunkSize(tmsg.Value)

	case *SetWindowAckSize:
		w.rmw.SetWindowAckSize(tmsg.Value)
	}

	return nil
}
