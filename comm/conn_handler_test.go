package comm

import (
	"github.com/LogicalOverflow/music-sync/logging"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"testing"
	"time"
)

func TestHandleConnection(t *testing.T) {
	log.DefaultCutoffLevel = log.LevelOff
	ch, cr := newPipeConnPair()

	tph := new(testPackageHandler)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		handleConnection(ch, tph)
	}()

	comm := make(chan bool, 1)

	go func() {
		defer wg.Done()
		for _, p := range testPackages {
			comm <- true
			result := tph.WaitForPackage()
			assert.Equal(t, p, result, "sending packages %v did not call package handler", p)
		}
	}()

	go func() {
		defer wg.Done()
		for _, p := range testPackages {
			<-comm
			sendWire(p, cr)
		}
		cr.Close()
	}()

	wg.Wait()
}

const invalidLastChannel = -32

func TestServerConnectionAcceptor(t *testing.T) {
	fl := newFakeListener()

	var lastChanMutex sync.Mutex
	var lastChan Channel = invalidLastChannel
	setLastChan := func(c Channel) {
		lastChanMutex.Lock()
		defer lastChanMutex.Unlock()
		lastChan = c
	}
	getLastChan := func() Channel {
		lastChanMutex.Lock()
		defer lastChanMutex.Unlock()
		return lastChan
	}
	NewClientHandler = func(channel Channel, conn MessageSender) { setLastChan(channel) }

	var wg sync.WaitGroup

	wg.Add(2)

	mms := &multiMessageSender{connections: make([]net.Conn, 0), channels: make(map[net.Conn][]Channel)}

	go func() {
		defer wg.Done()
		serverConnectionAcceptor(mms, fl)
	}()

	testConnsServer, testConnsClient := newPipeConnPairs(8)

	go func() {
		defer wg.Done()
		for i := range testConnsServer {
			fl.NewConn(testConnsServer[i])
			time.Sleep(20 * time.Millisecond)
			assert.True(t, containsConn(mms.snapshotConns(), testConnsServer[i]), "serverConnectionAcceptor did not AddConn (new) to mms (%d conns in mms)", len(mms.snapshotConns()))

			assert.Equal(t, Channel(-1), getLastChan(), "serverConnectionAcceptor did not call NewClientHandler with the correct channel")
			setLastChan(invalidLastChannel)

			testConnsClient[i].Close()
			time.Sleep(20 * time.Millisecond)
			assert.False(t, containsConn(mms.snapshotConns(), testConnsServer[i]), "serverConnectionAcceptor did not DelConn (closed) from mms (%d conns in mms)", len(mms.snapshotConns()))
		}
		fl.Close()
	}()

	wg.Wait()
}

// TestServerConnectionAcceptor_Subscription is an integration test for the server connection
// handling: a client subscribes to a channel through the wire protocol, receives messages of that
// channel, and after the connection is closed no subscription remains and no data is written to
// the closed connection anymore.
func TestServerConnectionAcceptor_Subscription(t *testing.T) {
	log.DefaultCutoffLevel = log.LevelOff
	NewClientHandler = func(channel Channel, conn MessageSender) {}

	fl := newFakeListener()
	mms := &multiMessageSender{connections: make([]net.Conn, 0), channels: make(map[net.Conn][]Channel)}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverConnectionAcceptor(mms, fl)
	}()

	serverConn, clientConn := newPipeConnPair()
	fl.NewConn(serverConn)

	// subscribe to the META channel through the wire protocol
	subscribed := make(chan struct{})
	go func() {
		sendWire(&SubscribeChannelRequest{Channel: Channel_META}, clientConn)
		close(subscribed)
	}()
	<-subscribed
	time.Sleep(50 * time.Millisecond)

	mms.mutex.RLock()
	_, isSubscribed := mms.channels[serverConn]
	mms.mutex.RUnlock()
	assert.True(t, isSubscribed, "subscribing through the wire protocol did not register the subscription")

	// broadcast a META message and read it from the client side. The pipe is unbuffered, so the
	// broadcast has to run concurrently to the read.
	p := &PauseInfo{Playing: true, ToggleSampleIndex: 1}
	sendDone := make(chan error, 1)
	go func() { sendDone <- mms.SendMessage(p) }()

	result, err := readWire(clientConn)
	if assert.Nil(t, err, "reading the broadcast message failed: %v", err) {
		assert.Equal(t, p.String(), result.String(), "the client received a different message than the broadcast message")
	}
	select {
	case err := <-sendDone:
		assert.Nil(t, err, "SendMessage returned an error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("SendMessage did not return")
	}

	// close the client side; the server has to clean up the connection and its subscription
	clientConn.Close()
	time.Sleep(100 * time.Millisecond)

	mms.mutex.RLock()
	_, stillSubscribed := mms.channels[serverConn]
	inConns := mms.hasConn(serverConn)
	mms.mutex.RUnlock()
	assert.False(t, stillSubscribed, "the subscription of a closed connection was not removed")
	assert.False(t, inConns, "a closed connection was not removed from the connection list")
	assert.False(t, mms.hasState(serverConn), "the state of a closed connection was not removed")

	// broadcasting after the connection was closed must not fail or block
	assert.Nil(t, mms.SendMessage(p), "broadcasting after a connection was closed returned an error")

	fl.Close()
	wg.Wait()
}

func containsConn(cs []net.Conn, c net.Conn) bool {
	for _, co := range cs {
		if co == c {
			return true
		}
	}
	return false
}
