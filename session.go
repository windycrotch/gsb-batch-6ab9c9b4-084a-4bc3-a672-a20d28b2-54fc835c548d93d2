package adler

import (
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lxzan/gws"
)

const (
	maxCloseReasonSize    = 123             // RFC 6455 allows 125 control-frame bytes, including the two-byte code.
	closeHandshakeTimeout = 2 * time.Second // Bound cleanup when a peer never answers the close frame.
)

// Session represents a single WebSocket client connection.
type Session struct {
	conn       *gws.Conn
	request    *http.Request
	protocol   string
	store      map[string]any
	output     chan message
	outputDone chan struct{}
	writeMu    sync.Mutex
	mu         sync.RWMutex
	identity   string
	closed     bool
	room       *Room
	adler      *Adler
}

// writeMessage enqueues an outbound message for asynchronous writing.
func (s *Session) writeMessage(message message) {
	if s.isClosed() {
		if s.adler.handlers.errorHandler != nil {
			s.adler.handlers.errorHandler(s, ErrWriteClosed)
		}
		return
	}

	select {
	case s.output <- message:
	default:
		if s.adler.handlers.errorHandler != nil {
			s.adler.handlers.errorHandler(s, ErrBufferFull)
		}
	}
}

// writeFrame writes a websocket frame directly to the client connection.
func (s *Session) writeFrame(message message) error {
	if s.isClosed() {
		return ErrWriteClosed
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	writeWait := message.writeWait
	// if writeWait is not specified in message struct it will be added from config
	if writeWait <= 0 {
		writeWait = s.adler.Config.WriteWait
	}

	if writeWait > 0 {
		if err := s.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
			return err
		}
		defer func() {
			_ = s.conn.SetWriteDeadline(time.Time{})
		}()
	}

	err := s.conn.WriteMessage(message.messageType, message.content)
	if err != nil {
		return err
	}

	switch message.messageType {
	case gws.OpcodeText:
		if s.adler.handlers.messageSentHandler != nil {
			s.adler.handlers.messageSentHandler(s, message.content)
		}
	case gws.OpcodeBinary:
		if s.adler.handlers.messageSentHandlerBinary != nil {
			s.adler.handlers.messageSentHandlerBinary(s, message.content)
		}
	}

	return nil
}

// writePump drains the output queue and periodically sends ping frames.
func (s *Session) writePump() {
	cfg := s.adler.Config
	pingPeriod := cfg.PingPeriod
	if pingPeriod <= 0 {
		pingPeriod = 54 * time.Second
	}

	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

loop:
	for {
		select {
		case message := <-s.output:
			err := s.writeFrame(message)
			if err != nil {
				if s.adler.handlers.errorHandler != nil {
					s.adler.handlers.errorHandler(s, err)
				}
				break loop
			}
		case <-ticker.C:
			s.ping()
		case _, ok := <-s.outputDone:
			if !ok {
				break loop
			}
		}
	}
}

// isClosed reports whether the session is already closed.
func (s *Session) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

// close marks the session as closed and releases connection resources once.
func (s *Session) close() {
	s.mu.Lock()
	closed := s.closed
	s.closed = true
	s.mu.Unlock()

	if !closed {
		s.conn.NetConn().Close()
		close(s.outputDone)
	}
}

// readPump continuously reads client frames and dispatches them to handlers.
func (s *Session) readPump() {
	dispatchAsync := s.adler.Config.DispatchAsync

	if wait := s.adler.Config.PongWait; wait > 0 {
		if err := s.conn.SetReadDeadline(time.Now().Add(wait)); err != nil {
			if handler := s.adler.handlers.errorHandler; handler != nil {
				handler(s, err)
			}
			return
		}
	}

	for {
		msg, err := s.conn.ReadMessage()
		if err != nil {
			if s.adler.handlers.errorHandler != nil {
				s.adler.handlers.errorHandler(s, err)
			}
			return
		}

		opcode := msg.Opcode
		payload := msg.Bytes()

		if dispatchAsync {
			cp := make([]byte, len(payload))
			copy(cp, payload)
			msg.Close()
			go s.handleMessage(opcode, cp)
		} else {
			s.handleMessage(opcode, payload)
			msg.Close()
		}
	}
}

// handleMessage routes text and binary frames to the configured handlers.
func (s *Session) handleMessage(op gws.Opcode, message []byte) {
	switch op {
	case gws.OpcodeText:
		if s.adler.handlers.messageHandler != nil {
			s.adler.handlers.messageHandler(s, message)
		}
	case gws.OpcodeBinary:
		if s.adler.handlers.messageHandlerBinary != nil {
			s.adler.handlers.messageHandlerBinary(s, message)
		}
	}
}

// ping sends a websocket ping control frame.
func (s *Session) ping() {
	s.conn.WritePing(nil)
}

// WriteText queues a text message to be sent to the client.
func (s *Session) WriteText(message []byte) error {
	return s.write(message, gws.OpcodeText)
}

// write adds the message in queue through writeMessage.
func (s *Session) write(msg []byte, code gws.Opcode, deadline ...time.Duration) error {
	if s.isClosed() {
		return ErrSessionClosed
	}

	m := message{
		messageType: code,
		content:     msg,
	}
	if len(deadline) > 0 {
		m.writeWait = deadline[0]
	}

	s.writeMessage(m)
	return nil
}

// WriteTextWithDeadline queues a text message with a custom write deadline.
func (s *Session) WriteTextWithDeadline(message []byte, deadline time.Duration) error {
	return s.write(message, gws.OpcodeText, deadline)
}

// WriteJSON marshals v to JSON and queues it as a text message.
func (s *Session) WriteJSON(v Map) error {
	jsonContent, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return s.write(jsonContent, gws.OpcodeText)
}

// Write marshals v using the configured serializer and queues it as a message.
// The serializer determines whether the message is sent as JSON (text) or Protobuf (binary).
func (s *Session) Write(v any) error {
	if s.adler.serializer == nil {
		return nil
	}

	data, opcode, err := s.adler.serializer.Marshal(v)
	if err != nil {
		return err
	}

	return s.write(data, opcode)
}

// WriteJSONWithDeadline queues a JSON payload with a custom write deadline.
func (s *Session) WriteJSONWithDeadline(message any, deadline time.Duration) error {
	jsonContent, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return s.write(jsonContent, gws.OpcodeText, deadline)
}

// WriteBinary queues a binary message to be sent to the client.
func (s *Session) WriteBinary(msg []byte) error {
	return s.write(msg, gws.OpcodeBinary)
}

// WriteBinaryWithDeadline queues a binary message with a custom write deadline.
func (s *Session) WriteBinaryWithDeadline(msg []byte, deadline time.Duration) error {
	return s.write(msg, gws.OpcodeBinary, deadline)
}

// Close queues a websocket close frame, optionally with a close payload.
func (s *Session) Close(msg ...[]byte) error {
	if s.isClosed() {
		return ErrSessionClosed
	}

	message := message{
		messageType: gws.OpcodeCloseConnection,
	}

	if len(msg) > 0 {
		message.content = msg[0]
	}

	s.writeMessage(message)
	return nil
}

// CloseWithCode queues a close frame and forces disconnection after two seconds.
// The caller must supply a valid WebSocket close code and UTF-8 reason.
// Reasons exceeding 123 bytes are truncated without splitting a UTF-8 rune.
func (s *Session) CloseWithCode(code uint16, reason string) error {
	if len(reason) > maxCloseReasonSize {
		limit := maxCloseReasonSize
		for limit > 0 && !utf8.RuneStart(reason[limit]) {
			limit--
		}
		reason = reason[:limit]
	}

	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload, code)
	copy(payload[2:], reason)
	if err := s.Close(payload); err != nil {
		return err
	}

	time.AfterFunc(closeHandshakeTimeout, s.close)
	return nil
}

// Set ensures session key storage is initialized.
func (s *Session) Set(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store == nil {
		s.store = make(map[string]any)
	}
	s.store[key] = value
}

// Get returns the value stored under key or ErrKeyNotFound.
func (s *Session) Get(key string) (any, error) {
	s.mu.RLock()
	val, exists := s.store[key]
	s.mu.RUnlock()
	if !exists {
		return nil, ErrKeyNotFound
	}

	return val, nil
}

// Unset removes key from the session store.
func (s *Session) Unset(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, key)
}

// Has reports whether key exists in the session store.
func (s *Session) Has(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.store[key]
	return exists
}

// SetNX sets key only when it does not already exist.
// It returns true when the value was written.
func (s *Session) SetNX(key string, value any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.store == nil {
		s.store = make(map[string]any)
	}

	_, exists := s.store[key]
	if exists {
		return false
	}

	s.store[key] = value
	return true
}

// Keys returns a snapshot of keys currently in the session store.
func (s *Session) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, len(s.store))
	i := 0

	for k := range s.store {
		keys[i] = k
		i++
	}
	return keys
}

// Values returns a snapshot of values currently in the session store.
func (s *Session) Values() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]any, len(s.store))
	i := 0

	for _, v := range s.store {
		values[i] = v
		i++
	}

	return values
}

// GetString returns the string value for key and true on success.
func (s *Session) GetString(key string) (string, bool) {
	s.mu.RLock()
	val, exists := s.store[key]
	s.mu.RUnlock()

	if !exists {
		return "", false
	}

	if v, ok := val.(string); ok {
		return v, true
	}

	return "", false
}

// GetInt returns the int value for key and true on success.
func (s *Session) GetInt(key string) (int, bool) {
	s.mu.RLock()
	val, exists := s.store[key]
	s.mu.RUnlock()

	if !exists {
		return 0, false
	}

	if v, ok := val.(int); ok {
		return v, true
	}

	return 0, false
}

// GetInt64 returns the int64 value for key and true on success.
func (s *Session) GetInt64(key string) (int64, bool) {
	s.mu.RLock()
	val, exists := s.store[key]
	s.mu.RUnlock()

	if !exists {
		return 0, false
	}
	if v, ok := val.(int64); ok {
		return v, true
	}
	return 0, false
}

// GetFloat returns the float64 value for key and true on success.
func (s *Session) GetFloat(key string) (float64, bool) {
	s.mu.RLock()
	val, exists := s.store[key]
	s.mu.RUnlock()

	if !exists {
		return 0, false
	}
	if v, ok := val.(float64); ok {
		return v, true
	}
	return 0, false
}

// GetBool returns the bool value for key and true on success.
func (s *Session) GetBool(key string) (bool, bool) {
	s.mu.RLock()
	val, exists := s.store[key]
	s.mu.RUnlock()

	if !exists {
		return false, false
	}
	if v, ok := val.(bool); ok {
		return v, true
	}
	return false, false
}

// Incr increments an int64 pointer value stored at key and returns the result.
func (s *Session) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ptr, exists := s.store[key]
	if !exists {
		return 0, ErrKeyNotFound
	}

	p, ok := ptr.(*int64)
	if !ok {
		return 0, ErrTypeAssertionFailed
	}
	*p++
	return *p, nil
}

// Decr decrements an int64 pointer value stored at key and returns the result.
func (s *Session) Decr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ptr, exists := s.store[key]
	if !exists {
		return 0, ErrKeyNotFound
	}

	p, ok := ptr.(*int64)
	if !ok {
		return 0, ErrTypeAssertionFailed
	}

	*p--
	return *p, nil
}

// Clear removes all key-value pairs from the session store.
func (s *Session) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.store)
}

// LocalAddr returns the local network address of the websocket connection.
func (s *Session) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

// RemoteAddr returns the remote network address of the websocket connection.
func (s *Session) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

// Room returns the room currently associated with the session.
func (s *Session) Room() *Room {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.room
}

// Request returns the original HTTP request used for websocket upgrade.
func (s *Session) Request() *http.Request {
	return s.request
}

// Protocol returns the HTTP protocol string from the upgrade request.
func (s *Session) Protocol() string {
	return s.protocol
}

// UnsafeConn exposes the underlying websocket network connection.
// Callers must coordinate concurrent reads/writes to avoid races.
func (s *Session) UnsafeConn() net.Conn {
	return s.conn.NetConn()
}
