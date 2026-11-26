package server

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/disgoorg/snowflake/v2"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/shi-gg/linkdave/server/audio/filter"
	"github.com/shi-gg/linkdave/server/protocol"
)

const (
	WRITE_TIMEOUT    = 10 * time.Second
	PONG_TIMEOUT     = 60 * time.Second
	PING_PERIOD      = (PONG_TIMEOUT * 9) / 10
	MAX_MESSAGE_SIZE = 512 * 1024 // 512KB
)

type Player struct {
	mutex       sync.RWMutex
	guildID     snowflake.ID
	channelID   snowflake.ID
	state       string
	currentURL  string
	position    int64
	startedAt   time.Time
	requesterID string
	filters     *filter.Filters

	// migrating marks an in-flight migration handoff. While set, snapshot is
	// the single authoritative, immutable handoff state; repeat handshakes
	// return it and stop/play transitions clear it.
	migrating bool
	snapshot  *protocol.MigrateReadyData
}

type Client struct {
	server     *Server
	conn       *websocket.Conn
	sendCh     chan any
	sessionID  string
	clientName string

	players map[snowflake.ID]*Player
	// handoffs keeps the immutable snapshot of every guild whose migration
	// handshake was served. It makes repeated handshakes return the exact
	// same state and lets lifecycle callbacks stay silent afterwards.
	handoffs map[snowflake.ID]protocol.MigrateReadyData
	// guildLocks serializes all same-guild entry points (voice update,
	// play/replace, stop/disconnect, migration) against each other.
	guildLocks map[snowflake.ID]*sync.Mutex
	playersMu  sync.RWMutex

	closeChan chan struct{}
	closeOnce sync.Once
}

func NewClient(server *Server, conn *websocket.Conn, clientName string) *Client {
	return &Client{
		server:     server,
		conn:       conn,
		sendCh:     make(chan any, 256),
		sessionID:  uuid.New().String(),
		clientName: clientName,
		players:    make(map[snowflake.ID]*Player),
		handoffs:   make(map[snowflake.ID]protocol.MigrateReadyData),
		guildLocks: make(map[snowflake.ID]*sync.Mutex),
		closeChan:  make(chan struct{}),
	}
}

// withGuildLock runs fn while holding the lock for a single guild, linearizing
// concurrent updates/play/disconnect operations for that player.
func (c *Client) withGuildLock(guildID snowflake.ID, fn func()) {
	c.playersMu.Lock()
	lock, ok := c.guildLocks[guildID]
	if !ok {
		lock = &sync.Mutex{}
		c.guildLocks[guildID] = lock
	}
	c.playersMu.Unlock()

	lock.Lock()
	defer lock.Unlock()
	fn()
}

func (c *Client) readPump() {
	defer func() {
		c.close()
	}()

	c.conn.SetReadLimit(MAX_MESSAGE_SIZE)
	c.conn.SetReadDeadline(time.Now().Add(PONG_TIMEOUT))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(PONG_TIMEOUT))
		return nil
	})

	for {
		msgType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.server.logger.Error("websocket read error", slog.Any("error", err))
			}
			return
		}
		c.server.handleMessage(c, msgType, message)
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(PING_PERIOD)
	defer func() {
		ticker.Stop()
		c.close()
	}()

	for {
		select {
		case message, ok := <-c.sendCh:
			c.conn.SetWriteDeadline(time.Now().Add(WRITE_TIMEOUT))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			data, err := json.Marshal(message)
			if err != nil {
				c.server.logger.Error("failed to marshal message", slog.Any("error", err))
				continue
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				c.server.logger.Error("failed to write message", slog.Any("error", err))
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(WRITE_TIMEOUT))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-c.closeChan:
			return
		}
	}
}

func (c *Client) send(msg protocol.Message) {
	select {
	case c.sendCh <- msg:
	default:
		c.server.logger.Warn("client send buffer full, dropping message")
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		c.conn.Close()
		c.server.unregisterClient(c)
		c.server.logger.Info("client disconnected", slog.String("session", c.sessionID))
	})
}

func (c *Client) getOrCreatePlayer(guildID snowflake.ID) *Player {
	c.playersMu.Lock()
	defer c.playersMu.Unlock()

	if player, ok := c.players[guildID]; ok {
		return player
	}

	player := &Player{
		guildID: guildID,
		state:   protocol.PlayerStateIdle,
	}
	c.players[guildID] = player
	return player
}

func (c *Client) getPlayer(guildID snowflake.ID) *Player {
	c.playersMu.RLock()
	defer c.playersMu.RUnlock()
	return c.players[guildID]
}

func (c *Client) removePlayer(guildID snowflake.ID) {
	c.playersMu.Lock()
	delete(c.players, guildID)
	c.playersMu.Unlock()
}

func (c *Client) addHandoff(guildID snowflake.ID, data protocol.MigrateReadyData) {
	c.playersMu.Lock()
	c.handoffs[guildID] = data
	c.playersMu.Unlock()
}

func (c *Client) getHandoff(guildID snowflake.ID) (protocol.MigrateReadyData, bool) {
	c.playersMu.RLock()
	data, ok := c.handoffs[guildID]
	c.playersMu.RUnlock()
	return data, ok
}

func (c *Client) removeHandoff(guildID snowflake.ID) (protocol.MigrateReadyData, bool) {
	c.playersMu.Lock()
	data, ok := c.handoffs[guildID]
	delete(c.handoffs, guildID)
	c.playersMu.Unlock()
	return data, ok
}

func (c *Client) isHandedOff(guildID snowflake.ID) bool {
	c.playersMu.RLock()
	_, ok := c.handoffs[guildID]
	c.playersMu.RUnlock()
	return ok
}

func (c *Client) destroyAllPlayers() {
	c.playersMu.Lock()
	guildIDs := make([]snowflake.ID, 0, len(c.players)+len(c.handoffs))
	for id := range c.players {
		guildIDs = append(guildIDs, id)
	}
	// Handoff guilds were removed from players but still own a voice
	// connection that must not leak when the socket goes away.
	for id := range c.handoffs {
		if _, active := c.players[id]; !active {
			guildIDs = append(guildIDs, id)
		}
	}
	c.players = make(map[snowflake.ID]*Player)
	c.handoffs = make(map[snowflake.ID]protocol.MigrateReadyData)
	c.playersMu.Unlock()

	for _, guildID := range guildIDs {
		c.server.voiceManager.Disconnect(c.sessionID, guildID)
	}
}

func (p *Player) GetState() string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.state
}

func (p *Player) SetState(state string) {
	p.mutex.Lock()
	p.state = state
	p.mutex.Unlock()
}

func (p *Player) GetCurrentURL() string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.currentURL
}

func (p *Player) SetCurrentURL(url string) {
	p.mutex.Lock()
	p.currentURL = url
	p.mutex.Unlock()
}

func (p *Player) GetPosition() int64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.position
}

func (p *Player) SetPosition(pos int64) {
	p.mutex.Lock()
	p.position = pos
	p.mutex.Unlock()
}

func (p *Player) GetStartedAt() time.Time {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.startedAt
}

func (p *Player) SetStartedAt(t time.Time) {
	p.mutex.Lock()
	p.startedAt = t
	p.mutex.Unlock()
}

func (p *Player) GetChannelID() snowflake.ID {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.channelID
}

func (p *Player) SetChannelID(id snowflake.ID) {
	p.mutex.Lock()
	p.channelID = id
	p.mutex.Unlock()
}

func (p *Player) SetPlayingState(url string, position int64, requesterID string, filters *filter.Filters) {
	p.mutex.Lock()
	p.state = protocol.PlayerStatePlaying
	p.currentURL = url
	p.position = position
	p.startedAt = time.Now()
	p.requesterID = requesterID
	p.filters = filters.Normalize()
	p.clearMigrationLocked()
	p.mutex.Unlock()
}

func (p *Player) GetRequesterID() string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.requesterID
}

func (p *Player) SetRequesterID(id string) {
	p.mutex.Lock()
	p.requesterID = id
	p.mutex.Unlock()
}

func (p *Player) SetIdleState() {
	p.mutex.Lock()
	p.state = protocol.PlayerStateIdle
	p.currentURL = ""
	p.requesterID = ""
	p.clearMigrationLocked()
	p.mutex.Unlock()
}

func (p *Player) SetPausedState(position int64) {
	p.mutex.Lock()
	p.state = protocol.PlayerStatePaused
	p.position = position
	p.mutex.Unlock()
}

func (p *Player) GetMigrateData() (url string, position int64, state string, requesterID string, filters *filter.Filters) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	calculatedPos := p.position
	if p.state == protocol.PlayerStatePlaying {
		calculatedPos = time.Since(p.startedAt).Milliseconds() + p.position
	}
	return p.currentURL, calculatedPos, p.state, p.requesterID, p.filters
}

// LivePosition returns the wall-clock-interpolated playback position. It is
// only a fallback when the voice connection cannot report a source position.
func (p *Player) LivePosition() int64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.state != protocol.PlayerStatePlaying {
		return p.position
	}

	return time.Since(p.startedAt).Milliseconds() + p.position
}

// BeginMigration freezes the immutable handoff snapshot. positionFn resolves
// the authoritative position (the frozen voice source position) while the
// player lock is held, so a concurrent pause/seek/play cannot mix old
// metadata with a new position. Idempotent: while a handoff is in flight the
// original snapshot is returned.
func (p *Player) BeginMigration(positionFn func() int64) protocol.MigrateReadyData {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.migrating {
		return *p.snapshot
	}

	position := positionFn()
	p.position = position
	p.migrating = true
	p.snapshot = &protocol.MigrateReadyData{
		GuildID:     p.guildID,
		URL:         p.currentURL,
		Position:    position,
		State:       p.state,
		RequesterID: p.requesterID,
		Filters:     p.filters.Normalize(),
	}

	return *p.snapshot
}

// MigrationData returns the frozen snapshot when a handoff is in flight.
func (p *Player) MigrationData() (protocol.MigrateReadyData, bool) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if !p.migrating || p.snapshot == nil {
		return protocol.MigrateReadyData{}, false
	}

	return *p.snapshot, true
}

func (p *Player) IsMigrating() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.migrating
}

// EndMigration aborts the handoff and restores wall-clock timing so playback
// continues from the frozen position without moving backwards.
func (p *Player) EndMigration() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.migrating || p.snapshot == nil {
		p.clearMigrationLocked()
		return
	}

	if p.state == protocol.PlayerStatePlaying {
		p.position = p.snapshot.Position
		p.startedAt = time.Now()
	} else if p.state == protocol.PlayerStatePaused {
		p.position = p.snapshot.Position
	}

	p.clearMigrationLocked()
}

func (p *Player) clearMigrationLocked() {
	p.migrating = false
	p.snapshot = nil
}

// RestoreFromHandoff rebuilds player state from a snapshot. It is used when a
// handoff record (rather than the in-player migration state) is aborted.
func (p *Player) RestoreFromHandoff(data protocol.MigrateReadyData) {
	p.mutex.Lock()
	p.state = data.State
	p.currentURL = data.URL
	p.position = data.Position
	p.requesterID = data.RequesterID
	p.filters = data.Filters.Normalize()
	p.clearMigrationLocked()

	if p.state == protocol.PlayerStatePlaying {
		p.startedAt = time.Now()
	}
	p.mutex.Unlock()
}
