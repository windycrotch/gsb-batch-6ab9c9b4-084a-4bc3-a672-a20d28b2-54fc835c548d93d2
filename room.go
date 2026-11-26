package adler

import (
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/lxzan/gws"
)

// Room groups sessions under the same logical channel.
type Room struct {
	mu       sync.RWMutex
	adler    *Adler
	handlers roomHandlers
	name     string
	sessions map[*Session]struct{}
	closed   atomic.Bool
}

type roomHandlers struct {
	onJoin  func(*Session)
	onLeave func(*Session)
}

func newRoom(name string, a *Adler) *Room {
	handlers := roomHandlers{}
	room := &Room{
		name:     name,
		sessions: make(map[*Session]struct{}),
		mu:       sync.RWMutex{},
		adler:    a,
		handlers: handlers,
	}
	room.closed.Store(false)
	return room
}

func (r *Room) isClosed() bool {
	return r.closed.Load()
}

// Name returns the room identifier.
func (r *Room) Name() string {
	return r.name
}

// Len returns the number of sessions currently in the room.
func (r *Room) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}

// Sessions returns a snapshot of current room members.
func (r *Room) Sessions() []*Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Session, 0, len(r.sessions))
	for k := range r.sessions {
		out = append(out, k)
	}
	return out
}

// OpenRoom marks the room as open for Join calls.
func (r *Room) OpenRoom() {
	r.closed.Store(false)
}

// CloseRoom marks the room as closed for new Join calls.
func (r *Room) CloseRoom() {
	r.closed.Store(true)
}

// Join adds s to the room and removes it from any previous room.
func (r *Room) Join(s *Session) error {
	if r.isClosed() {
		return ErrRoomClosed
	}
	if s == nil {
		return ErrNilSession
	}

	s.mu.Lock()
	r.mu.Lock()

	if r.isClosed() {
		r.mu.Unlock()
		s.mu.Unlock()
		return ErrRoomClosed
	}

	prev := s.room
	s.room = r

	if r.sessions == nil {
		r.sessions = make(map[*Session]struct{})
	}

	r.sessions[s] = struct{}{}
	r.mu.Unlock()
	s.mu.Unlock()

	if prev != nil && prev != r {
		prev.mu.Lock()
		delete(prev.sessions, s)
		prevEmpty := len(prev.sessions) == 0
		if prevEmpty {
			prev.sessions = nil
		}
		prev.mu.Unlock()

		if prev.handlers.onLeave != nil {
			prev.handlers.onLeave(s)
		}

		if prev.adler.handlers.onRoomLeave != nil {
			prev.adler.handlers.onRoomLeave(s, prev)
		}

		// remove previous room if it doesn't have sessions
		// to-do config flag
		if prevEmpty {
			prev.adler.removeRoomIfEmpty(prev)
		}
	}

	if r.handlers.onJoin != nil {
		r.handlers.onJoin(s)
	}

	if r.adler.handlers.onRoomJoin != nil {
		r.adler.handlers.onRoomJoin(s, r)
	}
	return nil
}

// Leave removes s from the room if s is currently a member.
func (r *Room) Leave(s *Session) {
	if s == nil {
		return
	}

	s.mu.Lock()
	if s.room != r {
		s.mu.Unlock()
		return
	}
	s.room = nil
	r.mu.Lock()
	delete(r.sessions, s)
	empty := len(r.sessions) == 0
	if empty {
		r.sessions = nil
	}
	r.mu.Unlock() // room mutex
	s.mu.Unlock() // session mutex

	if r.handlers.onLeave != nil {
		r.handlers.onLeave(s)
	}

	if r.adler.handlers.onRoomLeave != nil {
		r.adler.handlers.onRoomLeave(s, r)
	}

	if empty {
		r.adler.removeRoomIfEmpty(r)
	}
}

// HandleJoin registers a callback triggered after a session joins the room.
func (r *Room) HandleJoin(fn func(*Session)) {
	r.handlers.onJoin = fn
}

// HandleLeave registers a callback triggered after a session leaves the room.
func (r *Room) HandleLeave(fn func(*Session)) {
	r.handlers.onLeave = fn
}

// Broadcast sends a text websocket message to all room sessions.
func (r *Room) Broadcast(msg []byte) {
	r.broadcast(msg, gws.OpcodeText, nil)
}

// BroadcastFilter sends a text websocket message to room sessions matching filter.
func (r *Room) BroadcastFilter(msg []byte, filter func(*Session) bool) {
	r.broadcast(msg, gws.OpcodeText, filter)
}

// BroadcastOther sends a text websocket message to all room sessions excluding for s.
func (r *Room) BroadcastOther(msg []byte, s *Session) {
	r.BroadcastFilter(msg, func(other *Session) bool {
		return other != s
	})
}

// BroadcastBinary sends a binary websocket message to all room sessions.
func (r *Room) BroadcastBinary(msg []byte) {
	r.broadcast(msg, gws.OpcodeBinary, nil)
}

// BroadcastBinaryFilter sends a binary websocket message to room sessions matching filter.
func (r *Room) BroadcastBinaryFilter(msg []byte, filter func(*Session) bool) {
	r.broadcast(msg, gws.OpcodeBinary, filter)
}

// BroadcastBinaryOther sends a binary websocket message to all room sessions except for s.
func (r *Room) BroadcastBinaryOther(msg []byte, s *Session) {
	r.BroadcastBinaryFilter(msg, func(other *Session) bool {
		return other != s
	})
}

// BroadcastJSON marshals v and sends it as a text websocket message.
func (r *Room) BroadcastJSON(v Map) error {
	return r.BroadcastJSONFilter(v, nil)
}

// BroadcastJSONFilter marshals v and sends it as text to sessions matching filter.
func (r *Room) BroadcastJSONFilter(v Map, filter func(*Session) bool) error {
	content, err := json.Marshal(v)
	if err != nil {
		return err
	}

	r.broadcast(content, gws.OpcodeText, filter)
	return nil
}

// BroadcastAny marshals v using the configured serializer and broadcasts it to all room sessions.
// The serializer determines whether the message is sent as JSON (text) or Protobuf (binary).
func (r *Room) BroadcastAny(v any) error {
	if r.adler.serializer == nil {
		return nil
	}

	data, opcode, err := r.adler.serializer.Marshal(v)
	if err != nil {
		return err
	}

	r.broadcast(data, opcode, nil)
	return nil
}

func (r *Room) broadcast(content []byte, opCode gws.Opcode, filter func(*Session) bool) {
	r.mu.RLock()
	sessions := make([]*Session, 0, len(r.sessions))
	for session := range r.sessions {
		sessions = append(sessions, session)
	}
	r.mu.RUnlock()

	messageType := message{
		messageType: opCode,
		content:     content,
	}

	for _, session := range sessions {
		if filter == nil || filter(session) {
			session.writeMessage(messageType)
		}
	}
}
