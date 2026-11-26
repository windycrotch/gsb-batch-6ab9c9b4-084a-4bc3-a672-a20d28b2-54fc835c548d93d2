package adler

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/lxzan/gws"
)

type (
	messageFunc     func(*Session, []byte)
	errorFunc       func(*Session, error)
	closeFunc       func(*Session, int, string)
	sessionFunc     func(*Session)
	filterFunc      func(*Session) bool
	roomSessionFunc func(*Session, *Room)
)

type handlers struct {
	messageHandler           messageFunc
	messageHandlerBinary     messageFunc
	messageSentHandler       messageFunc
	messageSentHandlerBinary messageFunc
	errorHandler             errorFunc
	closeHandler             closeFunc
	connectHandler           sessionFunc
	disconnectHandler        sessionFunc
	disconnectHooks          []sessionFunc
	pongHandler              sessionFunc
	onRoomLeave              roomSessionFunc
	onRoomJoin               roomSessionFunc
}

// Adler is the main websocket server orchestrator.
// It owns active sessions, rooms and all user-provided callbacks.
type Adler struct {
	// Config holds runtime behavior knobs used by session read/write loops.
	Config     *Config
	serializer Serializer
	core       *core
	handlers   handlers
	hooksMu    sync.RWMutex
	rooms      map[string]*Room
	roomsMu    sync.RWMutex
	upgrader   *gws.Upgrader
}

// New initializes a ready-to-use Adler instance.
// Options override default configuration values in construction order.
func New(options ...Option) *Adler {
	// no handler functions are initialized by default, all are nil
	initHandlers := handlers{}
	cfg := newConfig(options...)

	var serializer Serializer
	switch cfg.Protocol {
	case JSON:
		serializer = JSONSerializer{}
	case Protobuf:
		serializer = ProtobufSerializer{}
	case None:
		serializer = nil
	default:
		serializer = nil
	}

	return &Adler{
		Config:     cfg,
		core:       newCore(),
		handlers:   initHandlers,
		rooms:      make(map[string]*Room),
		serializer: serializer,
		upgrader: gws.NewUpgrader(&adlerEventHandler{}, &gws.ServerOption{
			ReadMaxPayloadSize: cfg.ReadMaxPayloadSize,
		}),
	}
}

// HandleRequest upgrades the HTTP request to websocket and serves the session.
// The call blocks until the websocket session exits.
// Room membership and empty-room cleanup run before the disconnect callback.
func (a *Adler) HandleRequest(w http.ResponseWriter, r *http.Request) error {
	if a.core.isClosed() {
		return ErrCoreClosed
	}

	outputBufferSize := 1
	if a.Config != nil && a.Config.MessageBufferSize > 0 {
		outputBufferSize = int(a.Config.MessageBufferSize)
	}

	conn, err := a.upgradeWebSocket(r, w)
	if err != nil {
		return err
	}

	session := &Session{
		store:      make(map[string]any),
		request:    r,
		protocol:   r.Proto,
		conn:       conn,
		output:     make(chan message, outputBufferSize),
		outputDone: make(chan struct{}),
		mu:         sync.RWMutex{},
		closed:     false,
		adler:      a,
	}

	conn.Session().Store(sessionKey, session)

	if err := a.core.register(session); err != nil {
		_ = conn.NetConn().Close()
		return err
	}

	if a.handlers.connectHandler != nil {
		a.handlers.connectHandler(session)
	}
	go session.writePump()
	session.readPump()

	if room := session.Room(); room != nil {
		room.Leave(session)
	}

	// connection must be unregistered forced after closing readPump
	a.core.unregister(session)

	session.close()

	a.hooksMu.RLock()
	hooks := a.handlers.disconnectHooks
	handler := a.handlers.disconnectHandler
	a.hooksMu.RUnlock()

	for _, hook := range hooks {
		hook(session)
	}

	if handler != nil {
		handler(session)
	}
	return nil
}

func (a *Adler) broadcast(msg []byte, messageType gws.Opcode, fn ...func(*Session) bool) error {
	if a.core.isClosed() {
		return ErrCoreClosed
	}

	var filter func(*Session) bool

	if len(fn) > 0 {
		filter = fn[0]
	}

	a.core.broadcast(message{
		messageType: messageType,
		content:     msg,
		filter:      filter,
	})

	return nil
}

// Broadcast sends a text websocket message to all connected sessions.
func (a *Adler) Broadcast(msg []byte) error {
	return a.broadcast(msg, gws.OpcodeText)
}

// BroadcastFilter sends a text websocket message to sessions matching fn.
func (a *Adler) BroadcastFilter(msg []byte, fn func(*Session) bool) error {
	return a.broadcast(msg, gws.OpcodeText, fn)
}

// BroadcastBinary sends a binary websocket message to all connected sessions.
func (a *Adler) BroadcastBinary(msg []byte) error {
	return a.broadcast(msg, gws.OpcodeBinary)
}

// BroadcastBinaryFilter sends a binary websocket message to sessions matching fn.
func (a *Adler) BroadcastBinaryFilter(msg []byte, fn func(*Session) bool) error {
	return a.broadcast(msg, gws.OpcodeBinary, fn)
}

// BroadcastJSON marshals v and broadcasts it as a text websocket message.
func (a *Adler) BroadcastJSON(v Map) error {
	content, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return a.Broadcast(content)
}

// BroadcastJSONFilter marshals v and broadcasts it as text to sessions matching fn.
func (a *Adler) BroadcastJSONFilter(v Map, fn func(*Session) bool) error {
	content, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return a.BroadcastFilter(content, fn)
}

// Len returns the current number of active sessions.
func (a *Adler) Len() int {
	return a.core.len()
}

// IsClosed reports whether the server core has been closed.
func (a *Adler) IsClosed() bool {
	return a.core.isClosed()
}

// Close queues a close frame for all sessions and marks the core as closed.
func (a *Adler) Close() error {
	if a.core.isClosed() {
		return ErrCoreClosed
	}

	err := a.core.exit(message{
		messageType: gws.OpcodeCloseConnection,
		content:     []byte(""),
	})
	if err != nil {
		return ErrCoreExit
	}
	return nil
}

// NewRoom returns an existing room by name or creates a new one.
func (a *Adler) NewRoom(name string) *Room {
	a.roomsMu.Lock()
	defer a.roomsMu.Unlock()

	if a.rooms == nil {
		a.rooms = make(map[string]*Room)
	}

	if r, ok := a.rooms[name]; ok {
		return r
	}

	r := newRoom(name, a)
	a.rooms[name] = r
	return r
}

func (a *Adler) removeRoomIfEmpty(r *Room) {
	if !a.Config.DeleteRoomOnEmpty {
		return
	}

	if r == nil || r.Len() != 0 {
		return
	}

	a.roomsMu.Lock()
	defer a.roomsMu.Unlock()

	if a.rooms == nil {
		return
	}

	current, ok := a.rooms[r.name]
	if !ok || current != r {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.sessions) == 0 {
		r.closed.Store(true)
		delete(a.rooms, r.name)
	}
}

// DeleteRoom removes a room by name when it has no active sessions.
func (a *Adler) DeleteRoom(roomName string) error {
	a.roomsMu.Lock()
	defer a.roomsMu.Unlock()

	room, ok := a.rooms[roomName]
	if !ok {
		return ErrRoomNotFound
	}

	room.mu.Lock()
	defer room.mu.Unlock()

	if len(room.sessions) != 0 {
		return ErrRoomNotEmpty
	}

	room.closed.Store(true)
	room.sessions = nil
	delete(a.rooms, roomName)
	return nil
}

// BroadcastOthers sends msg to all connected sessions except s.
func (a *Adler) BroadcastOthers(msg []byte, s *Session) error {
	return a.BroadcastFilter(msg, func(other *Session) bool {
		return other != s
	})
}

// SendTo sends msg only to the target session s.
func (a *Adler) SendTo(msg []byte, s *Session) error {
	return a.BroadcastFilter(msg, func(other *Session) bool {
		return other == s
	})
}

func (a *Adler) upgradeWebSocket(r *http.Request, w http.ResponseWriter) (*gws.Conn, error) {
	return a.upgrader.Upgrade(w, r)
}

// OnRoomJoin registers a callback triggered after a session joins a room.
func (a *Adler) OnRoomJoin(fn func(*Session, *Room)) {
	a.handlers.onRoomJoin = fn
}

// OnRoomLeave registers a callback triggered after a session leaves a room.
func (a *Adler) OnRoomLeave(fn func(*Session, *Room)) {
	a.handlers.onRoomLeave = fn
}

// HandleConnect registers a callback triggered when a websocket session starts.
func (a *Adler) HandleConnect(fn func(*Session)) {
	a.handlers.connectHandler = fn
}

func (a *Adler) HandleClose(fn func(*Session, int, string)) {
	a.handlers.closeHandler = fn
}

// HandleDisconnect registers a callback triggered when a websocket session ends.
// It replaces any previously registered disconnect callback.
func (a *Adler) HandleDisconnect(fn func(*Session)) {
	a.hooksMu.Lock()
	defer a.hooksMu.Unlock()
	a.handlers.disconnectHandler = fn
}

// AddDisconnectHook registers an additional callback triggered when a websocket
// session ends, without replacing handlers registered through HandleDisconnect.
//
// Hooks run after the session is unregistered and closed, in registration order,
// and before the HandleDisconnect callback. Use it for infrastructure cleanup
// (for example, removing a session from matchmaking queues) that must not be
// lost when an application also registers its own disconnect handler.
func (a *Adler) AddDisconnectHook(fn func(*Session)) {
	if fn == nil {
		return
	}

	a.hooksMu.Lock()
	defer a.hooksMu.Unlock()
	a.handlers.disconnectHooks = append(a.handlers.disconnectHooks, fn)
}

// HandlePong registers a callback for pong events.
// When PongWait is enabled, the read deadline is renewed before fn is called.
func (a *Adler) HandlePong(fn func(*Session)) {
	a.handlers.pongHandler = fn
}

// HandleMessage registers the text-message handler.
func (a *Adler) HandleMessage(fn func(*Session, []byte)) {
	a.handlers.messageHandler = fn
}

// HandleMessageBinary registers the binary-message handler.
func (a *Adler) HandleMessageBinary(fn func(*Session, []byte)) {
	a.handlers.messageHandlerBinary = fn
}

// HandleSentMessage registers a callback for sent text messages.
func (a *Adler) HandleSentMessage(fn func(*Session, []byte)) {
	a.handlers.messageSentHandler = fn
}

// HandleSentMessageBinary registers a callback for sent binary messages.
func (a *Adler) HandleSentMessageBinary(fn func(*Session, []byte)) {
	a.handlers.messageSentHandlerBinary = fn
}

// HandleError registers the session-level error handler.
func (a *Adler) HandleError(fn func(*Session, error)) {
	a.handlers.errorHandler = fn
}

// GetRoom returns the room identified by RoomName
func (a *Adler) Room(roomName string) (*Room, error) {
	a.roomsMu.RLock()
	defer a.roomsMu.RUnlock()
	room, ok := a.rooms[roomName]
	if !ok {
		return nil, ErrRoomNotFound
	}
	return room, nil
}

// BroadcastAny marshals v using the configured serializer and broadcasts it to all connected sessions.
// The serializer determines whether the message is sent as JSON (text) or Protobuf (binary).
func (a *Adler) BroadcastAny(v any) error {
	if a.serializer == nil {
		return nil
	}

	data, opcode, err := a.serializer.Marshal(v)
	if err != nil {
		return err
	}

	return a.broadcast(data, opcode)
}

// sessionKey is the key used to store *Session in the gws Conn's SessionStorage.
const sessionKey = "adler:session"

// adlerEventHandler bridges gws Event callbacks to the Adler handler system.
// Only control-frame events (OnPing, OnPong, OnClose) are handled here; data
// frames are read via Conn.ReadMessage in the session readPump.
type adlerEventHandler struct{}

func (adlerEventHandler) OnOpen(socket *gws.Conn) {}

func (adlerEventHandler) OnClose(socket *gws.Conn, err error) {
	s := getSessionFromConn(socket)
	if s == nil {
		return
	}
	if s.adler.handlers.closeHandler != nil {
		if ce, ok := err.(*gws.CloseError); ok {
			s.adler.handlers.closeHandler(s, int(ce.Code), string(ce.Reason))
		}
	}
}

func (adlerEventHandler) OnPing(socket *gws.Conn, payload []byte) {
	_ = socket.WritePong(payload)
}

func (adlerEventHandler) OnPong(socket *gws.Conn, payload []byte) {
	s := getSessionFromConn(socket)
	if s == nil {
		return
	}

	if wait := s.adler.Config.PongWait; wait > 0 {
		if err := socket.SetReadDeadline(time.Now().Add(wait)); err != nil {
			if handler := s.adler.handlers.errorHandler; handler != nil {
				handler(s, err)
			}
			return
		}
	}

	if handler := s.adler.handlers.pongHandler; handler != nil {
		handler(s)
	}
}

func (adlerEventHandler) OnMessage(socket *gws.Conn, message *gws.Message) {}

func getSessionFromConn(socket *gws.Conn) *Session {
	v, ok := socket.Session().Load(sessionKey)
	if !ok {
		return nil
	}
	s, _ := v.(*Session)
	return s
}
