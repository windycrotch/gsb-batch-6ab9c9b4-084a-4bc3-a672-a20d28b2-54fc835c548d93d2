package rawmessage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
)

func TestResetClearsChunkLayerState(t *testing.T) {
	bcr := bytecounter.NewReader(bytes.NewReader(nil))
	r := &Reader{
		BR:          bcr,
		BCR:         bcr,
		OnAckNeeded: func(uint32) error { return nil },
	}
	r.Initialize()

	require.NoError(t, r.SetChunkSize(65536))
	r.SetWindowAckSize(2500000)
	r.LastAckCount = 1234
	r.chunkStreams[6] = &readerChunkStream{
		curBodyLen:       300,
		curBodyFragments: [][]byte{{0x01}},
		curBodyRecv:      1,
	}

	r.Reset()

	require.Equal(t, uint32(128), r.chunkSize)
	require.Zero(t, r.ackWindowSize)
	require.Zero(t, r.LastAckCount)
	require.Empty(t, r.chunkStreams)

	var buf bytes.Buffer
	bcw := bytecounter.NewWriter(&buf)
	w := &Writer{
		BW:               bcw,
		BCW:              bcw,
		CheckAcknowledge: true,
	}
	w.Initialize()

	w.SetChunkSize(65536)
	w.SetWindowAckSize(2500000)
	w.SetAcknowledgeValue(1234)
	w.chunkStreams[6] = &writerChunkStream{mw: w}

	w.Reset()

	require.Equal(t, uint32(128), w.chunkSize)
	require.Zero(t, w.ackWindowSize)
	require.Zero(t, w.AckValue)
	require.Empty(t, w.chunkStreams)
}
