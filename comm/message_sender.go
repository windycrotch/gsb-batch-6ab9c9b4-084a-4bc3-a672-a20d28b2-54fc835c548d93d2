package comm

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LogicalOverflow/music-sync/util"

	"github.com/golang/protobuf/proto"
)

// MessageSender is an interface used to send protobuf messages to clients/the server.
type MessageSender interface {
	// Sends a message to the server/all clients.
	// This method respects subscribe message from clients.
	SendMessage(m proto.Message) error
}

// connState is the state of a single connection. It is used to serialize writes to the same
// connection and to make sure that no data is written to a connection after it was closed.
type connState struct {
	mutex  sync.Mutex
	closed int32
}

// isClosed returns whether the connection is closed
func (cs *connState) isClosed() bool {
	return atomic.LoadInt32(&cs.closed) != 0
}

// markClosed marks the connection as closed and returns whether this call performed the
// transition (i.e. the connection was not closed before)
func (cs *connState) markClosed() bool {
	return atomic.CompareAndSwapInt32(&cs.closed, 0, 1)
}

// closeConn marks the connection as closed, aborts a write that is currently in progress and
// closes the underlying connection. After closeConn returns, no write is in progress and no new
// write can start on the connection: write checks the closed state while holding the write lock,
// and this function marks the state closed before it waits for the write lock and closes the
// connection.
func (cs *connState) closeConn(conn net.Conn) {
	// Marking the state closed first makes sure that a write which is aborted below does not
	// report an error (closing the connection is intentional, not a failed send) and that no new
	// write can start after this point.
	cs.markClosed()

	// Abort a write that is currently blocked on a slow/unresponsive peer, so closing a connection
	// does not block the server or the other connections of a broadcast.
	conn.SetWriteDeadline(time.Now())

	// Wait for a write that is in progress to release the write lock, then close the connection.
	// This order guarantees that no write is started after the connection was closed.
	cs.mutex.Lock()
	cs.mutex.Unlock()

	conn.Close()
}

// closedConnState returns a connState that is already closed. It is used for connections that are
// not registered (anymore), which must not be written to.
func closedConnState() *connState {
	cs := &connState{}
	cs.markClosed()
	return cs
}

// write writes data to conn. It returns nil without writing anything if the connection was
// already closed. Writes to the same connection are serialized.
func (cs *connState) write(conn net.Conn, data []byte) error {
	// Fast path: do not even try to write to a connection that is known to be closed.
	if cs.isClosed() {
		return nil
	}

	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	// The connection could have been closed while waiting for the write lock.
	if cs.isClosed() {
		return nil
	}

	if err := writeAll(conn, data); err != nil {
		// If the connection was closed while the data was written, the error is expected and must
		// not be reported as a failure to send to the connection.
		if cs.isClosed() {
			return nil
		}
		return err
	}
	return nil
}

type multiMessageSender struct {
	connections []net.Conn
	channels    map[net.Conn][]Channel
	mutex       sync.RWMutex

	// states contains the write/close state of the connections. It is guarded by statesMutex
	// instead of mutex, so the state of a connection can be looked up during a broadcast without
	// blocking subscription changes or connection removals of other connections.
	//
	// Lock order: mutex (if acquired) before statesMutex. state mutexes are never acquired while
	// holding one of the other locks.
	statesMutex sync.Mutex
	states      map[net.Conn]*connState
}

// sendTarget is a connection a broadcast has to be sent to together with its state at the time the
// broadcast started.
type sendTarget struct {
	conn  net.Conn
	state *connState
}

func (mms *multiMessageSender) SendMessage(m proto.Message) error {
	chs, hasCh := channelOf(m)

	// Serialize the message only once, all connections get the same wire data.
	data, err := toWire(m)
	if err != nil {
		return err
	}

	// Determine the target connections and evaluate their subscriptions at a single point in time.
	// The lock is released before anything is written, so closing/adding connections or changing
	// subscriptions does not have to wait for a broadcast to finish.
	//
	// Because of the snapshot, a connection that subscribes while the broadcast is running is not
	// part of this broadcast (it receives all subsequent ones) and a connection that is closed
	// while the broadcast is running is skipped by the write (its state is marked closed) instead
	// of being written to.
	mms.mutex.RLock()
	targets := make([]sendTarget, 0, len(mms.connections))
	for _, c := range mms.connections {
		// Messages without a channel are sent to every connection. Messages with a channel are
		// only sent to connections subscribed to (at least) one of the message channels.
		if hasCh && !mms.isSubscribed(c, chs) {
			continue
		}
		targets = append(targets, sendTarget{conn: c, state: mms.stateOf(c)})
	}
	mms.mutex.RUnlock()

	// Send the message to all targets concurrently, so a slow or failing connection does not
	// prevent the other connections from receiving the message. Errors are collected per target
	// and returned as one MultiError.
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	wg.Add(len(targets))

	for i, t := range targets {
		go func(i int, t sendTarget) {
			defer wg.Done()
			errs[i] = t.state.write(t.conn, data)
		}(i, t)
	}

	wg.Wait()

	failed := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			failed = append(failed, err)
		}
	}
	if len(failed) == 0 {
		return nil
	}
	return util.NewMultiError(fmt.Sprintf("failed to send to %d clients", len(failed)), failed)
}

// stateOf returns the state of conn, creating it if it does not exist yet. mutex or statesMutex
// have to be held by the caller.
func (mms *multiMessageSender) stateOf(c net.Conn) *connState {
	mms.statesMutex.Lock()
	defer mms.statesMutex.Unlock()

	if mms.states == nil {
		mms.states = make(map[net.Conn]*connState)
	}
	if s, ok := mms.states[c]; ok {
		return s
	}
	s := &connState{}
	mms.states[c] = s
	return s
}

// takeState removes and returns the state of conn (nil if there is none). mutex has to be held by
// the caller.
func (mms *multiMessageSender) takeState(c net.Conn) *connState {
	mms.statesMutex.Lock()
	defer mms.statesMutex.Unlock()

	s := mms.states[c]
	delete(mms.states, c)
	return s
}

func (mms *multiMessageSender) AddConn(c net.Conn) {
	mms.mutex.Lock()
	defer mms.mutex.Unlock()

	if mms.hasConn(c) {
		return
	}

	mms.connections = append(mms.connections, c)
	// Create the state while the connection is added, so it exists before the first broadcast.
	mms.stateOf(c)
}

func (mms *multiMessageSender) DelConn(c net.Conn) {
	mms.mutex.Lock()

	index := -1
	for i, conn := range mms.connections {
		if conn == c {
			index = i
			break
		}
	}

	if index < 0 {
		mms.mutex.Unlock()
		return
	}

	last := len(mms.connections) - 1
	mms.connections[index] = mms.connections[last]
	mms.connections[last] = nil
	mms.connections = mms.connections[:last]

	// Remove the subscriptions of the connection, so nothing is left behind that could be sent to
	// a closed connection or become active again if the connection value is reused.
	delete(mms.channels, c)

	state := mms.takeState(c)
	if state != nil {
		// Mark the connection as closed while the sender lock is still held, so a broadcast that
		// already determined its targets cannot start writing to the connection anymore.
		state.markClosed()
	}
	mms.mutex.Unlock()

	if state != nil {
		// A write that is currently in progress is aborted by closing the connection; because the
		// state was marked closed above, that write swallows the resulting error instead of
		// reporting it as a failed send.
		state.closeConn(c)
	} else {
		c.Close()
	}
}

func (mms *multiMessageSender) Subscribe(c net.Conn, channel Channel) {
	mms.mutex.Lock()
	defer mms.mutex.Unlock()

	// Only connections that are currently registered can subscribe. A subscription request that
	// arrives after the connection was closed/removed is ignored, so no subscription state is left
	// behind for it.
	if !mms.hasConn(c) {
		return
	}

	if mms.channels == nil {
		mms.channels = make(map[net.Conn][]Channel)
	}

	// Subscribing to a channel multiple times must not cause messages to be sent multiple times.
	if containsChannel(mms.channels[c], channel) {
		return
	}
	mms.channels[c] = append(mms.channels[c], channel)
}

// hasConn returns whether c is currently registered. mutex has to be held by the caller.
func (mms *multiMessageSender) hasConn(c net.Conn) bool {
	for _, conn := range mms.connections {
		if conn == c {
			return true
		}
	}
	return false
}

// snapshotConns returns a copy of the current connection list. It is used by tests.
func (mms *multiMessageSender) snapshotConns() []net.Conn {
	mms.mutex.RLock()
	defer mms.mutex.RUnlock()
	return append([]net.Conn{}, mms.connections...)
}

// hasState returns whether a state exists for c. It is used by tests.
func (mms *multiMessageSender) hasState(c net.Conn) bool {
	mms.statesMutex.Lock()
	defer mms.statesMutex.Unlock()
	_, ok := mms.states[c]
	return ok
}

// isSubscribed returns whether the connection c is subscribed to at least one of channels. mutex
// has to be held by the caller.
func (mms *multiMessageSender) isSubscribed(c net.Conn, channels []Channel) bool {
	if l, ok := mms.channels[c]; ok {
		for _, ch := range l {
			if containsChannel(channels, ch) {
				return true
			}
		}
	}
	return false
}

func containsChannel(channels []Channel, ch Channel) bool {
	for _, c := range channels {
		if ch == c {
			return true
		}
	}
	return false
}

// writeToConn writes m to the single (registered) connection c. It is used by the server to
// answer requests of a client. Like the broadcast, it never writes to a connection that was closed
// or removed (the message is discarded in that case).
func (mms *multiMessageSender) writeToConn(c net.Conn, m proto.Message) error {
	data, err := toWire(m)
	if err != nil {
		return err
	}

	mms.mutex.RLock()
	registered := mms.hasConn(c)
	var state *connState
	if registered {
		state = mms.stateOf(c)
	}
	mms.mutex.RUnlock()

	if !registered {
		return nil
	}
	return state.write(c, data)
}

// senderFor returns a MessageSender that writes to the single connection c. The sender shares the
// write state of the connection with the broadcasts, so writes to the same connection never
// interleave and no data is written to c after it was closed. If c is not registered (anymore),
// the returned sender silently discards all messages.
func (mms *multiMessageSender) senderFor(c net.Conn) MessageSender {
	mms.mutex.RLock()
	defer mms.mutex.RUnlock()

	if mms.hasConn(c) {
		return &singleMessageSender{connection: c, state: mms.stateOf(c)}
	}
	return &singleMessageSender{connection: c, state: closedConnState()}
}

type singleMessageSender struct {
	connection net.Conn
	state      *connState
}

func (sms *singleMessageSender) SendMessage(m proto.Message) error {
	data, err := toWire(m)
	if err != nil {
		return err
	}

	if sms.state != nil {
		return sms.state.write(sms.connection, data)
	}
	return writeAll(sms.connection, data)
}
