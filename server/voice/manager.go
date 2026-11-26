package voice

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/disgoorg/disgo/voice"
	"github.com/disgoorg/snowflake/v2"
	"github.com/shi-gg/linkdave/server/audio/filter"
	"github.com/shi-gg/linkdave/server/audio/source"
	"github.com/shi-gg/linkdave/server/protocol"
)

func connectionKey(sessionID string, guildID snowflake.ID) string {
	return sessionID + ":" + guildID.String()
}

type EventHandler interface {
	OnTrackEnd(sessionID string, guildID snowflake.ID, src source.Source, reason string)
	OnTrackException(sessionID string, guildID snowflake.ID, src source.Source, err error)
	OnVoiceDisconnected(sessionID string, guildID snowflake.ID)
}

// SourceFactory creates audio sources for playback. It is a seam so tests can
// run against purely local sources instead of fetching real audio.
type SourceFactory interface {
	CreateFromURL(ctx context.Context, url string, startTimeMs int64, filters *filter.Filters) (source.Source, error)
}

type Manager struct {
	logger       *slog.Logger
	connections  map[string]*Connection
	mutex        sync.RWMutex
	eventHandler EventHandler

	connCreate    voice.ConnCreateFunc
	sourceFactory SourceFactory
}

func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		logger:        logger,
		connections:   make(map[string]*Connection),
		connCreate:    voice.NewConn,
		sourceFactory: source.NewDefaultFactory(),
	}
}

// SetConnCreateFunc overrides how Discord voice connections are created.
// Intended for tests, where a local fake replaces the real connection.
func (m *Manager) SetConnCreateFunc(connCreate voice.ConnCreateFunc) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.connCreate = connCreate
}

// SetSourceFactory overrides audio source creation. Intended for tests, so no
// external audio or network access is required.
func (m *Manager) SetSourceFactory(factory SourceFactory) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.sourceFactory = factory
}

func (m *Manager) SetEventHandler(handler EventHandler) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.eventHandler = handler
}

func (m *Manager) onTrackEnd(sessionID string, guildID snowflake.ID, src source.Source, reason string, err error) {
	m.mutex.RLock()
	handler := m.eventHandler
	m.mutex.RUnlock()

	if handler == nil {
		return
	}

	if reason == protocol.TrackEndReasonError {
		handler.OnTrackException(sessionID, guildID, src, err)
	}

	handler.OnTrackEnd(sessionID, guildID, src, reason)
}

func (m *Manager) onDisconnect(sessionID string, guildID snowflake.ID, conn *Connection, key string) {
	m.mutex.Lock()
	if m.connections[key] != conn {
		m.mutex.Unlock()
		return
	}
	delete(m.connections, key)
	handler := m.eventHandler
	m.mutex.Unlock()

	// A committed migration owns this disconnect: the player now lives on
	// another node, so the old node must not advertise it as lost.
	if conn.IsHandedOff() || handler == nil {
		return
	}

	handler.OnVoiceDisconnected(sessionID, guildID)
}

func (m *Manager) Connect(ctx context.Context, sessionID string, userID, guildID, channelID snowflake.ID, discordSessionID string, event protocol.VoiceServerEvent) error {
	key := connectionKey(sessionID, guildID)

	m.mutex.Lock()
	existing, ok := m.connections[key]
	connCreate := m.connCreate
	m.mutex.Unlock()

	if ok {
		return existing.HandleVoiceUpdate(ctx, channelID, discordSessionID, event)
	}

	var conn *Connection
	conn, err := NewConnection(ctx, m.logger, connCreate, userID, guildID, channelID, discordSessionID, event,
		func(src source.Source, reason string, err error) {
			m.onTrackEnd(sessionID, guildID, src, reason, err)
		},
		func() {
			m.onDisconnect(sessionID, guildID, conn, key)
		},
	)

	if err != nil {
		return fmt.Errorf("failed to create voice connection: %w", err)
	}

	m.mutex.Lock()
	if existing, ok := m.connections[key]; ok {
		m.mutex.Unlock()
		conn.Close()
		return existing.HandleVoiceUpdate(ctx, channelID, discordSessionID, event)
	}
	m.connections[key] = conn
	m.mutex.Unlock()

	return nil
}

func (m *Manager) getConnection(sessionID string, guildID snowflake.ID) *Connection {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.connections[connectionKey(sessionID, guildID)]
}

// ConnectionCount reports the number of live voice connections across all
// sessions. Primarily useful for observability and tests.
func (m *Manager) ConnectionCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.connections)
}

// HasConnection reports whether a connection is currently registered for the
// session/guild pair.
func (m *Manager) HasConnection(sessionID string, guildID snowflake.ID) bool {
	return m.getConnection(sessionID, guildID) != nil
}

func (m *Manager) Play(ctx context.Context, sessionID string, guildID snowflake.ID, url string, startTime int64, filters *filter.Filters) (source.Source, error) {
	m.mutex.RLock()
	conn := m.connections[connectionKey(sessionID, guildID)]
	factory := m.sourceFactory
	m.mutex.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("no voice connection for guild %s", guildID)
	}

	src, err := factory.CreateFromURL(ctx, url, startTime, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to create audio source: %w", err)
	}

	if err := conn.Play(ctx, src); err != nil {
		src.Close()
		return nil, err
	}

	return src, nil
}

func (m *Manager) Pause(sessionID string, guildID snowflake.ID) error {
	conn := m.getConnection(sessionID, guildID)
	if conn == nil {
		return fmt.Errorf("no voice connection for guild %s", guildID)
	}

	conn.Pause()
	return nil
}

func (m *Manager) Resume(sessionID string, guildID snowflake.ID) error {
	conn := m.getConnection(sessionID, guildID)
	if conn == nil {
		return fmt.Errorf("no voice connection for guild %s", guildID)
	}

	conn.Resume()
	return nil
}

func (m *Manager) Stop(sessionID string, guildID snowflake.ID) error {
	conn := m.getConnection(sessionID, guildID)
	if conn == nil {
		return fmt.Errorf("no voice connection for guild %s", guildID)
	}

	conn.Stop()
	return nil
}

func (m *Manager) Seek(sessionID string, guildID snowflake.ID, position int64) error {
	conn := m.getConnection(sessionID, guildID)
	if conn == nil {
		return fmt.Errorf("no voice connection for guild %s", guildID)
	}

	return conn.SeekTo(position)
}

func (m *Manager) Position(sessionID string, guildID snowflake.ID) int64 {
	conn := m.getConnection(sessionID, guildID)
	if conn == nil {
		return 0
	}

	return conn.Position()
}

// BeginMigration freezes the connection at its real playback position and
// returns that position. ok is false when no voice connection exists.
func (m *Manager) BeginMigration(sessionID string, guildID snowflake.ID) (position int64, ok bool) {
	conn := m.getConnection(sessionID, guildID)
	if conn == nil {
		return 0, false
	}

	return conn.BeginMigration(), true
}

// EndMigration aborts a pending handoff and resumes playback.
func (m *Manager) EndMigration(sessionID string, guildID snowflake.ID) {
	if conn := m.getConnection(sessionID, guildID); conn != nil {
		conn.EndMigration()
	}
}

// CommitMigration silences the connection's lifecycle events and drops it
// from the manager. The actual close happens asynchronously.
func (m *Manager) CommitMigration(sessionID string, guildID snowflake.ID) error {
	key := connectionKey(sessionID, guildID)

	m.mutex.Lock()
	conn, ok := m.connections[key]
	if !ok {
		m.mutex.Unlock()
		return nil
	}
	delete(m.connections, key)
	m.mutex.Unlock()

	conn.MarkHandedOff()

	go func() {
		conn.Close()
		m.logger.Info("migrated voice connection closed",
			slog.String("session", sessionID),
			slog.String("guild_id", guildID.String()),
		)
	}()

	return nil
}

func (m *Manager) Disconnect(sessionID string, guildID snowflake.ID) error {
	key := connectionKey(sessionID, guildID)

	m.mutex.Lock()

	conn, ok := m.connections[key]
	if !ok {
		m.mutex.Unlock()
		return nil
	}

	delete(m.connections, key)
	m.mutex.Unlock()

	go func() {
		conn.Close()

		m.logger.Info("voice connection closed",
			slog.String("session", sessionID),
			slog.String("guild_id", guildID.String()),
		)
	}()

	return nil
}

func (m *Manager) Close() {
	m.mutex.Lock()
	connections := m.connections
	m.connections = make(map[string]*Connection)
	m.mutex.Unlock()

	// Close outside the manager lock: teardown callbacks re-enter the manager
	// (onDisconnect), which would self-deadlock with a held write lock.
	for _, conn := range connections {
		conn.Close()
	}
}
