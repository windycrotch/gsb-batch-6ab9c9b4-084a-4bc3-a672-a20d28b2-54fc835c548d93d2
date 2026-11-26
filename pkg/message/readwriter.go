package message

import (
	"io"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
)

// ReadWriter is a message reader/writer.
type ReadWriter struct {
	r *Reader
	w *Writer
}

// NewReadWriter allocates a ReadWriter.
func NewReadWriter(
	rw io.ReadWriter,
	bcrw *bytecounter.ReadWriter,
	checkAcknowledge bool,
) *ReadWriter {
	w := &Writer{
		BW:               rw,
		BCW:              bcrw.Writer,
		CheckAcknowledge: checkAcknowledge,
	}
	w.Initialize()

	r := &Reader{
		BR:  rw,
		BCR: bcrw.Reader,
		OnAckNeeded: func(count uint32) error {
			return w.Write(&Acknowledge{
				Value: count,
			})
		},
	}
	r.Initialize()

	return &ReadWriter{
		r: r,
		w: w,
	}
}

// Reset resets the underlying chunk reader and writer to their initial state.
// The maximum chunk size goes back to the default value, the window
// acknowledgement size and the last received acknowledge are cleared and any
// message that was being assembled from multiple chunks is discarded.
func (rw *ReadWriter) Reset() {
	rw.r.Reset()
	rw.w.Reset()
}

// Read reads a message.
func (rw *ReadWriter) Read() (Message, error) {
	msg, err := rw.r.Read()
	if err != nil {
		return nil, err
	}

	switch tmsg := msg.(type) {
	case *Acknowledge:
		rw.w.SetAcknowledgeValue(tmsg.Value)

	case *UserControlPingRequest:
		err = rw.w.Write(&UserControlPingResponse{
			ServerTime: tmsg.ServerTime,
		})
		if err != nil {
			return nil, err
		}
	}

	return msg, nil
}

// Write writes a message.
func (rw *ReadWriter) Write(msg Message) error {
	return rw.w.Write(msg)
}
