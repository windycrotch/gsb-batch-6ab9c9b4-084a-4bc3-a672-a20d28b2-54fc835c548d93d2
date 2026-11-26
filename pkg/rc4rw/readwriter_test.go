package rc4rw_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib/pkg/rc4rw"
)

var _ io.ReadWriter = (*rc4rw.ReadWriter)(nil)

func TestReadWriterRoundTrip(t *testing.T) {
	// client and server ciphers must be mirrored: what one side encrypts, the
	// other side decrypts, and the initial 1536-byte discard must happen
	// exactly once per direction (otherwise the streams would be offset).
	keyIn := bytes.Repeat([]byte{0x01}, 16)
	keyOut := bytes.Repeat([]byte{0x02}, 16)

	var a2b bytes.Buffer
	var b2a bytes.Buffer

	client, err := rc4rw.New(&duplex{
		reader: &b2a,
		writer: &a2b,
	}, keyIn, keyOut)
	require.NoError(t, err)

	server, err := rc4rw.New(&duplex{
		reader: &a2b,
		writer: &b2a,
	}, keyOut, keyIn)
	require.NoError(t, err)

	payloads := [][]byte{
		bytes.Repeat([]byte{0xAA}, 1),
		bytes.Repeat([]byte{0xBB}, 1536),
		bytes.Repeat([]byte{0xCC}, 9000),
		bytes.Repeat([]byte{0xDD}, 37),
	}

	for _, payload := range payloads {
		_, err = client.Write(append([]byte(nil), payload...))
		require.NoError(t, err)

		buf := make([]byte, len(payload))
		_, err = io.ReadFull(server, buf)
		require.NoError(t, err)
		require.Equal(t, payload, buf)

		_, err = server.Write(append([]byte(nil), payload...))
		require.NoError(t, err)

		_, err = io.ReadFull(client, buf)
		require.NoError(t, err)
		require.Equal(t, payload, buf)
	}
}

type duplex struct {
	reader io.Reader
	writer io.Writer
}

func (d *duplex) Read(p []byte) (int, error)  { return d.reader.Read(p) }
func (d *duplex) Write(p []byte) (int, error) { return d.writer.Write(p) }
