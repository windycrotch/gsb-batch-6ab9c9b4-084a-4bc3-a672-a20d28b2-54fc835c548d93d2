package comm

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LogicalOverflow/music-sync/util"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

// controllableConn is a net.Conn that records everything written to it. Writes can be blocked
// (to observe a broadcast that is in progress) and can be made to fail (to test error
// aggregation). It also records writes that *start* after the connection was closed.
type controllableConn struct {
	name string

	closedFlag int32
	mutex      sync.Mutex
	buf        bytes.Buffer
	closed     bool

	// writesAfterClose counts writes that started after Close was called
	writesAfterClose int
	// started is closed when a write begins (if set), release is waited on before the data is
	// written (if set).
	started chan struct{}
	release chan struct{}
	// abort is closed by SetWriteDeadline with a deadline in the past, it aborts a blocked write.
	abort chan struct{}

	// failing causes every write to return an error
	failing bool
}

func (c *controllableConn) Read([]byte) (int, error)        { return 0, fmt.Errorf("not readable") }
func (c *controllableConn) LocalAddr() net.Addr             { return fakeAddr{c.name} }
func (c *controllableConn) RemoteAddr() net.Addr            { return fakeAddr{c.name} }
func (c *controllableConn) SetDeadline(time.Time) error     { return nil }
func (c *controllableConn) SetReadDeadline(time.Time) error { return nil }
func (c *controllableConn) SetWriteDeadline(t time.Time) error {
	if c.abort != nil && !t.After(time.Now()) {
		select {
		case <-c.abort:
		default:
			close(c.abort)
		}
	}
	return nil
}

func (c *controllableConn) Write(b []byte) (int, error) {
	// Decide at the beginning of the write whether it started after the connection was closed.
	startedAfterClose := atomic.LoadInt32(&c.closedFlag) != 0

	if c.started != nil {
		select {
		case <-c.started:
		default:
			close(c.started)
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if startedAfterClose {
		c.writesAfterClose++
		return 0, fmt.Errorf("connection %s is closed", c.name)
	}

	if c.release != nil {
		select {
		case <-c.release:
		case <-c.abort:
			return 0, fmt.Errorf("connection %s: write aborted", c.name)
		}
	}

	if c.failing {
		return 0, fmt.Errorf("connection %s always fails", c.name)
	}
	return c.buf.Write(b)
}

func (c *controllableConn) Close() error {
	atomic.StoreInt32(&c.closedFlag, 1)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return nil
}

func (c *controllableConn) bytes() []byte {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return append([]byte{}, c.buf.Bytes()...)
}

func (c *controllableConn) isClosed() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.closed
}

func (c *controllableConn) writtenAfterClose() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.writesAfterClose
}

func newControllableConn(name string) *controllableConn {
	return &controllableConn{name: name, abort: make(chan struct{})}
}

func newMultiMessageSender(conns ...net.Conn) *multiMessageSender {
	mms := &multiMessageSender{connections: make([]net.Conn, 0, len(conns)), channels: make(map[net.Conn][]Channel)}
	for _, c := range conns {
		mms.AddConn(c)
	}
	return mms
}

// TestMultiMessageSender_UnsubscribedConnectionReceivesNoChannelMessage makes sure a connection
// that did not subscribe to any channel does not receive any message that has a channel.
func TestMultiMessageSender_UnsubscribedConnectionReceivesNoChannelMessage(t *testing.T) {
	for i, p := range testPackages {
		if len(testPackageChannels[i]) == 0 {
			continue
		}

		unsubscribed := newControllableConn("unsubscribed")
		mms := newMultiMessageSender(unsubscribed)

		err := mms.SendMessage(p)
		assert.Nil(t, err, "SendMessage returned an error for package %v: %v", p, err)
		assert.Zero(t, len(unsubscribed.bytes()), "multiMessageSender sent package %v to a connection that is not subscribed to any channel", p)
	}
}

// TestMultiMessageSender_SubscribedConnectionReceivesOnlySubscribedChannels makes sure a connection
// only receives messages of the channels it subscribed to.
func TestMultiMessageSender_SubscribedConnectionReceivesOnlySubscribedChannels(t *testing.T) {
	for i, p := range testPackages {
		expected, err := toWire(p)
		if !assert.Nil(t, err, "toWire returned an error for package %v: %v", p, err) {
			continue
		}

		audio := newControllableConn("audio")
		meta := newControllableConn("meta")
		mms := newMultiMessageSender(audio, meta)
		mms.Subscribe(audio, Channel_AUDIO)
		mms.Subscribe(meta, Channel_META)

		mms.SendMessage(p)

		hasAudio, hasMeta := hasChannels(testPackageChannels[i])
		audio.assertWrote(t, expected, hasAudio, p)
		meta.assertWrote(t, expected, hasMeta, p)
	}
}

func (c *controllableConn) assertWrote(t *testing.T, expected []byte, shouldWrite bool, p proto.Message) {
	if shouldWrite {
		assert.True(t, bytes.Equal(expected, c.bytes()), "multiMessageSender did not (correctly) write to connection %s for package %v", c.name, p)
	} else {
		assert.Zero(t, len(c.bytes()), "multiMessageSender wrote to connection %s for package %v although it is not subscribed to its channel", c.name, p)
	}
}

// TestMultiMessageSender_DelConnCleansUp makes sure closing a connection removes its subscription
// record, closes the connection and stops all writes to it.
func TestMultiMessageSender_DelConnCleansUp(t *testing.T) {
	conn := newControllableConn("conn")
	other := newControllableConn("other")
	mms := newMultiMessageSender(conn, other)
	mms.Subscribe(conn, Channel_META)
	mms.Subscribe(other, Channel_META)

	mms.DelConn(conn)

	_, hasChannels := mms.channels[conn]
	assert.False(t, hasChannels, "DelConn did not remove the subscription records of the connection")
	assert.True(t, conn.isClosed(), "DelConn did not close the connection")

	p := &PauseInfo{Playing: true, ToggleSampleIndex: 1}
	err := mms.SendMessage(p)
	assert.Nil(t, err, "SendMessage returned an error after a connection was removed: %v", err)
	assert.Zero(t, len(conn.bytes()), "multiMessageSender wrote to a connection after it was removed")
	assert.Zero(t, conn.writtenAfterClose(), "multiMessageSender wrote to a closed connection")

	expected, _ := toWire(p)
	assert.True(t, bytes.Equal(expected, other.bytes()), "removing a connection prevented another connection from receiving the message")
}

// TestMultiMessageSender_DuplicateSubscribe makes sure subscribing to the same channel multiple
// times neither duplicates the subscription nor causes messages to be sent multiple times, and
// that a subscription does not survive the removal of the connection.
func TestMultiMessageSender_DuplicateSubscribe(t *testing.T) {
	conn := newControllableConn("conn")
	mms := newMultiMessageSender(conn)

	mms.Subscribe(conn, Channel_META)
	mms.Subscribe(conn, Channel_META)
	mms.Subscribe(conn, Channel_META)
	assert.Equal(t, 1, len(mms.channels[conn]), "duplicate subscriptions were recorded multiple times")

	p := &ChunkInfo{StartTime: 1, FirstSampleIndex: 2, ChunkSize: 3}
	mms.SendMessage(p)
	expected, _ := toWire(p)
	assert.True(t, bytes.Equal(expected, conn.bytes()), "connection did not receive exactly one copy of the message")

	mms.DelConn(conn)

	// A subscription request for a connection that is not registered must be ignored and must not
	// leave state behind that would be used when the connection is added again.
	mms.Subscribe(conn, Channel_META)
	_, hasChannels := mms.channels[conn]
	assert.False(t, hasChannels, "Subscribe recorded a subscription for a connection that is not registered")

	reconnected := newControllableConn("conn")
	mms.AddConn(reconnected)
	mms.SendMessage(p)
	assert.Zero(t, len(reconnected.bytes()), "a subscription from before the removal of the connection is still active after reconnecting")
}

// TestMultiMessageSender_SendMessageErrorIsolation makes sure a failing connection does not
// prevent other connections from receiving the message and that all failures are reported.
func TestMultiMessageSender_SendMessageErrorIsolation(t *testing.T) {
	failing1 := newControllableConn("failing-1")
	failing1.failing = true
	failing2 := newControllableConn("failing-2")
	failing2.failing = true
	working := newControllableConn("working")

	mms := newMultiMessageSender(failing1, failing2, working)
	for _, c := range []net.Conn{failing1, failing2, working} {
		mms.Subscribe(c, Channel_AUDIO)
	}

	p := &QueueChunkRequest{StartTime: 1, ChunkId: 2, FirstSampleIndex: 3}
	err := mms.SendMessage(p)

	expected, _ := toWire(p)
	assert.True(t, bytes.Equal(expected, working.bytes()), "a failing connection prevented another connection from receiving the message")
	assert.NotNil(t, err, "SendMessage did not return an error although connections failed")

	var me util.MultiError
	if assert.True(t, errors.As(err, &me), "SendMessage did not return a MultiError") {
		assert.Equal(t, 2, len(me.Errors), "SendMessage reported the wrong number of failed connections")
	}
}

// TestMultiMessageSender_BroadcastDuringDelConn makes sure a broadcast that is already running
// does not write to a connection that is closed while the broadcast is in progress and that the
// closed connection does not cause an error or block the other connections.
func TestMultiMessageSender_BroadcastDuringDelConn(t *testing.T) {
	blocked := newControllableConn("blocked")
	blocked.started = make(chan struct{})
	blocked.release = make(chan struct{})
	other := newControllableConn("other")

	mms := newMultiMessageSender(blocked, other)
	mms.Subscribe(blocked, Channel_META)
	mms.Subscribe(other, Channel_META)

	p := &PauseInfo{Playing: true, ToggleSampleIndex: 1}
	done := make(chan error, 1)
	go func() { done <- mms.SendMessage(p) }()

	// Wait until the broadcast is writing to the blocked connection, then close it.
	<-blocked.started
	mms.DelConn(blocked)
	assert.True(t, blocked.isClosed(), "DelConn did not close the blocked connection")

	close(blocked.release)
	select {
	case err := <-done:
		assert.Nil(t, err, "SendMessage reported an error for a connection that was closed during the broadcast: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("SendMessage did not return after the connection was closed during the broadcast")
	}

	assert.Zero(t, len(blocked.bytes()), "multiMessageSender wrote to a connection that was closed during the broadcast")

	expected, _ := toWire(p)
	assert.True(t, bytes.Equal(expected, other.bytes()), "closing a connection during the broadcast prevented another connection from receiving the message")
}

// TestMultiMessageSender_SubscribeDuringBroadcast makes sure a connection that subscribes while a
// broadcast is running does not receive that broadcast, but all following ones.
func TestMultiMessageSender_SubscribeDuringBroadcast(t *testing.T) {
	blocked := newControllableConn("blocked")
	blocked.started = make(chan struct{})
	blocked.release = make(chan struct{})
	late := newControllableConn("late")

	mms := newMultiMessageSender(blocked, late)
	mms.Subscribe(blocked, Channel_META)

	p := &PauseInfo{Playing: true, ToggleSampleIndex: 1}
	done := make(chan error, 1)
	go func() { done <- mms.SendMessage(p) }()

	<-blocked.started
	mms.Subscribe(late, Channel_META)
	close(blocked.release)

	select {
	case err := <-done:
		assert.Nil(t, err, "SendMessage returned an error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("SendMessage did not return")
	}

	assert.Zero(t, len(late.bytes()), "a connection that subscribed during the broadcast received that broadcast")

	// The subscription has to be active for subsequent broadcasts.
	p2 := &ChunkInfo{StartTime: 1, FirstSampleIndex: 2, ChunkSize: 3}
	assert.Nil(t, mms.SendMessage(p2), "SendMessage returned an error")
	expected, _ := toWire(p2)
	assert.True(t, bytes.Equal(expected, late.bytes()), "a connection that subscribed during a broadcast did not receive the following broadcast")
}

// TestMultiMessageSender_ConcurrentDelivery makes sure the message is sent to all connections
// concurrently, i.e. a connection that blocks while it is written to does not delay the other
// connections.
func TestMultiMessageSender_ConcurrentDelivery(t *testing.T) {
	blocked := newControllableConn("blocked")
	blocked.started = make(chan struct{})
	blocked.release = make(chan struct{})
	fast := newControllableConn("fast")

	mms := newMultiMessageSender(blocked, fast)
	mms.Subscribe(blocked, Channel_AUDIO)
	mms.Subscribe(fast, Channel_AUDIO)

	p := &QueueChunkRequest{StartTime: 1, ChunkId: 2, FirstSampleIndex: 3}
	done := make(chan error, 1)
	go func() { done <- mms.SendMessage(p) }()

	// Wait until the blocked connection is being written to.
	<-blocked.started

	// The other connection has to receive the message although the broadcast is not finished.
	expected, _ := toWire(p)
	deadline := time.Now().Add(5 * time.Second)
	for len(fast.bytes()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	assert.True(t, bytes.Equal(expected, fast.bytes()), "the message was not sent concurrently to all connections")

	close(blocked.release)
	select {
	case err := <-done:
		assert.Nil(t, err, "SendMessage returned an error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("SendMessage did not return")
	}
	assert.True(t, bytes.Equal(expected, blocked.bytes()), "the blocked connection did not receive the message after it was unblocked")
}

// TestMultiMessageSender_NoWriteAfterClose makes sure single senders created for a connection do
// not write to the connection after it was removed either.
func TestMultiMessageSender_NoWriteAfterClose(t *testing.T) {
	conn := newControllableConn("conn")
	mms := newMultiMessageSender(conn)

	sender := mms.senderFor(conn)
	mms.DelConn(conn)

	err := sender.SendMessage(&PauseInfo{Playing: true, ToggleSampleIndex: 1})
	assert.Nil(t, err, "sending to a closed connection returned an error: %v", err)
	assert.Zero(t, len(conn.bytes()), "a sender wrote to a connection after it was removed")

	// Sending again to the removed connection must still not write anything.
	assert.Nil(t, mms.SendMessage(&ChunkInfo{StartTime: 1}), "SendMessage returned an error")
	assert.Zero(t, conn.writtenAfterClose(), "multiMessageSender wrote to a closed connection")
}

// TestMultiMessageSender_ConcurrentCloseAndSubscribe stresses concurrent broadcasts with
// connection removals and subscriptions. It must never write to a closed connection, must never
// panic and must keep delivering to connections that stay connected.
func TestMultiMessageSender_ConcurrentCloseAndSubscribe(t *testing.T) {
	stable := newControllableConn("stable")
	mms := newMultiMessageSender(stable)
	mms.Subscribe(stable, Channel_META)
	mms.Subscribe(stable, Channel_AUDIO)

	p := &PauseInfo{Playing: true, ToggleSampleIndex: 1}
	expected, _ := toWire(p)

	stop := make(chan struct{})
	var broadcaster sync.WaitGroup
	broadcaster.Add(1)
	go func() {
		defer broadcaster.Done()
		for {
			select {
			case <-stop:
				return
			default:
				mms.SendMessage(p)
			}
		}
	}()

	// Clients that connect, subscribe (repeatedly), get broadcasted to and disconnect.
	for i := 0; i < 200; i++ {
		c := newControllableConn("churn")
		mms.AddConn(c)
		mms.Subscribe(c, Channel_META)
		mms.Subscribe(c, Channel_AUDIO)
		mms.Subscribe(c, Channel_AUDIO)

		time.Sleep(time.Millisecond) // give the broadcaster a chance to see the connection

		mms.DelConn(c)
		assert.True(t, c.isClosed(), "DelConn did not close the connection")
		assert.Zero(t, c.writtenAfterClose(), "multiMessageSender wrote to a connection after it was closed (iteration %d)", i)
	}

	close(stop)
	broadcaster.Wait()

	data := stable.bytes()
	assert.True(t, len(data) > 0, "the stable connection did not receive any broadcast")
	assert.Zero(t, len(data)%len(expected), "the stable connection received a partial broadcast")
	assert.True(t, bytes.HasPrefix(data, expected), "the stable connection received corrupted data")
	assert.Zero(t, stable.writtenAfterClose(), "multiMessageSender wrote to the stable connection after it was closed")
}
