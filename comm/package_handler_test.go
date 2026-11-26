package comm

import (
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"testing"
)

type testTypedPackageHandler struct {
	lastPackage proto.Message
	lastType    string
	cond        sync.Cond
}

// setPackage stores the package and its type and wakes up a waiting test. The cond lock is used
// to synchronize the access with the test that reads the fields after Wait returns.
func (t *testTypedPackageHandler) setPackage(p proto.Message, pType string) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	t.lastPackage = p
	t.lastType = pType
	t.cond.Broadcast()
}

func (t *testTypedPackageHandler) HandleTimeSyncRequest(p *TimeSyncRequest, _ net.Conn) {
	t.setPackage(p, "TimeSyncRequest")
}
func (t *testTypedPackageHandler) HandleTimeSyncResponse(p *TimeSyncResponse, _ net.Conn) {
	t.setPackage(p, "TimeSyncResponse")
}
func (t *testTypedPackageHandler) HandleQueueChunkRequest(p *QueueChunkRequest, _ net.Conn) {
	t.setPackage(p, "QueueChunkRequest")
}
func (t *testTypedPackageHandler) HandlePingMessage(p *PingMessage, _ net.Conn) {
	t.setPackage(p, "PingMessage")
}
func (t *testTypedPackageHandler) HandlePongMessage(p *PongMessage, _ net.Conn) {
	t.setPackage(p, "PongMessage")
}
func (t *testTypedPackageHandler) HandleSetVolumeRequest(p *SetVolumeRequest, _ net.Conn) {
	t.setPackage(p, "SetVolumeRequest")
}
func (t *testTypedPackageHandler) HandleSubscribeChannelRequest(p *SubscribeChannelRequest, _ net.Conn) {
	t.setPackage(p, "SubscribeChannelRequest")
}
func (t *testTypedPackageHandler) HandleNewSongInfo(p *NewSongInfo, _ net.Conn) {
	t.setPackage(p, "NewSongInfo")
}
func (t *testTypedPackageHandler) HandleChunkInfo(p *ChunkInfo, _ net.Conn) {
	t.setPackage(p, "ChunkInfo")
}
func (t *testTypedPackageHandler) HandlePauseInfo(p *PauseInfo, _ net.Conn) {
	t.setPackage(p, "PauseInfo")
}

var typedPackageHandlerHandleCases = []struct {
	pType string
	p     proto.Message
}{
	{pType: "TimeSyncRequest", p: &TimeSyncRequest{ClientSend: 1}},
	{pType: "TimeSyncResponse", p: &TimeSyncResponse{ClientSendTime: 1, ServerRecvTime: 2, ServerSendTime: 3}},
	{pType: "QueueChunkRequest", p: &QueueChunkRequest{StartTime: 1, ChunkId: 2, FirstSampleIndex: 3}},
	{pType: "PingMessage", p: &PingMessage{}},
	{pType: "PongMessage", p: &PongMessage{}},
	{pType: "SetVolumeRequest", p: &SetVolumeRequest{Volume: 1.2}},
	{pType: "SubscribeChannelRequest", p: &SubscribeChannelRequest{Channel: Channel_AUDIO}},
	{pType: "NewSongInfo", p: &NewSongInfo{FirstSampleOfSongIndex: 1, SongFileName: "abc", SongLength: 2}},
	{pType: "ChunkInfo", p: &ChunkInfo{StartTime: 1, FirstSampleIndex: 2, ChunkSize: 3}},
	{pType: "PauseInfo", p: &PauseInfo{Playing: true, ToggleSampleIndex: 2}},
}

func TestTypedPackageHandler_Handle(t *testing.T) {
	ttph := &testTypedPackageHandler{cond: sync.Cond{L: new(sync.Mutex)}}
	ph := TypedPackageHandler{TypedPackageHandlerInterface: ttph}

	for _, c := range typedPackageHandlerHandleCases {
		done := make(chan bool)
		go func() {
			ttph.cond.L.Lock()
			done <- true
			ttph.cond.Wait()
			done <- true
			ttph.cond.L.Unlock()
		}()
		<-done
		ph.Handle(c.p, nil)
		<-done
		assert.Equal(t, c.pType, ttph.lastType, "handling package of type %s called handler for %s through TypedPackageHandler", c.pType, ttph.lastType)
		assert.Equal(t, c.p, ttph.lastPackage, "handling package of type %s called handler with wrong package through TypedPackageHandler", c.pType)
	}
}
