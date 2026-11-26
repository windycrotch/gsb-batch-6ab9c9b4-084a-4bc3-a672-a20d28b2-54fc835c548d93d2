package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/disgoorg/snowflake/v2"
	"github.com/gorilla/websocket"
	"github.com/shi-gg/linkdave/server/audio/source"
	"github.com/shi-gg/linkdave/server/protocol"
	"github.com/shi-gg/linkdave/server/voice"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now
	},
}

type Server struct {
	logger       *slog.Logger
	voiceManager *voice.Manager
	clients      map[string]*Client
	clientsMu    sync.RWMutex
	startTime    time.Time
	draining     bool
	drainMu      sync.RWMutex
	// admitMu linearizes drain transitions against in-flight player
	// admissions (play creation + mount): Drain takes the write side, each
	// admission takes the read side and re-checks draining.
	admitMu  sync.RWMutex
	version  string
	password string
}

func NewServer(logger *slog.Logger, voiceManager *voice.Manager, version string, password string) *Server {
	s := &Server{
		logger:       logger,
		voiceManager: voiceManager,
		clients:      make(map[string]*Client),
		startTime:    time.Now(),
		version:      version,
		password:     password,
	}
	voiceManager.SetEventHandler(s)
	s.startTickers()
	return s
}

func (s *Server) startTickers() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			s.sendStats()
		}
	}()
}

func (s *Server) sendStats() {
	stats := s.GetStats()
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	for _, client := range s.clients {
		client.send(protocol.Message{
			Op:   protocol.OpStats,
			Data: stats,
		})
	}
}

func (s *Server) OnTrackEnd(sessionID string, guildID snowflake.ID, src source.Source, reason string) {
	client := s.getClientBySession(sessionID)
	if client == nil {
		return
	}

	// A handed-off player belongs to the new node; its old node must not
	// report the track as ended.
	if client.isHandedOff(guildID) {
		return
	}

	player := client.getPlayer(guildID)
	if player == nil {
		return
	}

	track := protocol.TrackInfo{
		URL:         src.URL(),
		Duration:    src.Duration(),
		RequesterID: player.GetRequesterID(),
	}

	if reason != protocol.TrackEndReasonReplaced && reason != protocol.TrackEndReasonStopped {
		player.SetIdleState()
	}

	client.send(protocol.Message{
		Op: protocol.OpTrackEnd,
		Data: protocol.TrackEndData{
			GuildID: guildID,
			Track:   track,
			Reason:  reason,
		},
	})
}

func (s *Server) OnTrackException(sessionID string, guildID snowflake.ID, src source.Source, err error) {
	client := s.getClientBySession(sessionID)
	if client == nil {
		return
	}

	player := client.getPlayer(guildID)

	track := protocol.TrackInfo{
		URL:      src.URL(),
		Duration: src.Duration(),
	}

	if player != nil {
		track.RequesterID = player.GetRequesterID()
	}

	client.send(protocol.Message{
		Op: protocol.OpTrackError,
		Data: protocol.TrackErrorData{
			GuildID: guildID,
			Track:   track,
			Error:   err.Error(),
		},
	})
}

func (s *Server) OnVoiceDisconnected(sessionID string, guildID snowflake.ID) {
	client := s.getClientBySession(sessionID)
	if client == nil {
		return
	}

	// Migration already handed this guild to another node, or a handoff is
	// in flight: stay silent and keep the player so the handoff can commit or
	// abort instead of reporting it as connection-lost.
	if client.isHandedOff(guildID) {
		return
	}

	if player := client.getPlayer(guildID); player != nil && player.IsMigrating() {
		return
	}

	client.removePlayer(guildID)

	client.send(protocol.Message{
		Op: protocol.OpVoiceDisconnect,
		Data: protocol.VoiceDisconnectData{
			GuildID: guildID,
			Reason:  protocol.DisconnectReasonConnectionLost,
		},
	})
}

func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	if s.IsDraining() {
		http.Error(w, "Node is draining", http.StatusServiceUnavailable)
		return
	}

	if s.password != "" && r.URL.Query().Get("password") != s.password {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	clientName := r.Header.Get("Client-Name")
	if clientName == "" {
		clientName = "unknown"
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("failed to upgrade websocket", slog.Any("error", err))
		return
	}

	client := NewClient(s, conn, clientName)
	s.registerClient(client)

	s.logger.Info("client connected",
		slog.String("client", clientName),
		slog.String("session", client.sessionID),
		slog.String("addr", r.RemoteAddr),
	)

	client.send(protocol.Message{
		Op: protocol.OpReady,
		Data: protocol.ReadyData{
			SessionID: client.sessionID,
			Resumed:   false,
		},
	})

	go client.readPump()
	go client.writePump()
}

func (s *Server) registerClient(client *Client) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	s.clients[client.sessionID] = client
}

func (s *Server) unregisterClient(client *Client) {
	s.clientsMu.Lock()
	delete(s.clients, client.sessionID)
	s.clientsMu.Unlock()

	// Clean up all voice connections for this client's players
	go client.destroyAllPlayers()
}

func (s *Server) getClientBySession(sessionID string) *Client {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()
	return s.clients[sessionID]
}

func (s *Server) handleMessage(client *Client, msgType int, data []byte) {
	if msgType != websocket.TextMessage {
		return
	}

	var msg struct {
		Op   uint8           `json:"op"`
		Data json.RawMessage `json:"d"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Error("failed to unmarshal message", slog.Any("error", err))
		return
	}

	switch msg.Op {
	case protocol.OpVoiceUpdate:
		s.handleVoiceUpdate(client, msg.Data)
	case protocol.OpPlayerMigrate:
		s.handlePlayerMigrate(client, msg.Data)
	case protocol.OpPlayerMigrateCommit:
		s.handlePlayerMigrateCommit(client, msg.Data)
	case protocol.OpPlayerMigrateAbort:
		s.handlePlayerMigrateAbort(client, msg.Data)
	default:
		s.logger.Warn("unknown op code", slog.Uint64("op", uint64(msg.Op)))
	}
}

func (s *Server) handleVoiceUpdate(client *Client, data json.RawMessage) {
	var update protocol.VoiceUpdateData
	if err := json.Unmarshal(data, &update); err != nil {
		s.logger.Error("failed to unmarshal voice update", slog.Any("error", err))
		return
	}

	s.logger.Info("voice update received",
		slog.String("guild_id", update.GuildID.String()),
		slog.String("channel_id", update.ChannelID.String()),
	)

	client.withGuildLock(update.GuildID, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := s.voiceManager.Connect(ctx, client.sessionID, update.ClientID, update.GuildID, update.ChannelID, update.SessionID, update.Event)
		if err != nil {
			s.logger.Error("failed to connect to voice", slog.Any("error", err))
			client.removePlayer(update.GuildID)
			client.removeHandoff(update.GuildID)
			if disconnectErr := s.voiceManager.Disconnect(client.sessionID, update.GuildID); disconnectErr != nil {
				s.logger.Error("failed to clean up failed voice connection", slog.Any("error", disconnectErr))
			}

			client.send(protocol.Message{
				Op: protocol.OpVoiceDisconnect,
				Data: protocol.VoiceDisconnectData{
					GuildID: update.GuildID,
					Reason:  protocol.DisconnectReasonConnectionFailed,
				},
			})

			return
		}

		player := client.getOrCreatePlayer(update.GuildID)
		player.SetChannelID(update.ChannelID)

		client.send(protocol.Message{
			Op: protocol.OpVoiceConnect,
			Data: protocol.VoiceConnectData{
				GuildID:   update.GuildID,
				ChannelID: update.ChannelID,
			},
		})
	})
}

func (s *Server) handlePlayerMigrate(client *Client, data json.RawMessage) {
	var migrate protocol.PlayerMigrateData
	if err := json.Unmarshal(data, &migrate); err != nil {
		s.logger.Error("failed to unmarshal player migrate", slog.Any("error", err))
		return
	}

	var (
		snapshot protocol.MigrateReadyData
		found    bool
	)

	client.withGuildLock(migrate.GuildID, func() {
		// A committed handoff only keeps a suppression tombstone: answer
		// late repeat handshakes with the same last snapshot.
		if existing, ok := client.getHandoff(migrate.GuildID); ok {
			snapshot = existing
			found = true
			return
		}

		player := client.getPlayer(migrate.GuildID)
		if player == nil {
			return
		}

		if existing, ok := player.MigrationData(); ok {
			snapshot = existing
			found = true
			return
		}

		livePosition := player.LivePosition()
		snapshot = player.BeginMigration(func() int64 {
			position, ok := s.voiceManager.BeginMigration(client.sessionID, migrate.GuildID)
			if !ok {
				return livePosition
			}
			return position
		})
		found = true
	})

	if !found {
		s.logger.Warn("player not found for migration", slog.String("guild_id", migrate.GuildID.String()))
		client.send(protocol.Message{
			Op: protocol.OpMigrateFailed,
			Data: protocol.MigrateFailedData{
				GuildID: migrate.GuildID,
				Reason:  "player not found",
			},
		})
		return
	}

	client.send(protocol.Message{
		Op:   protocol.OpMigrateReady,
		Data: snapshot,
	})

	s.logger.Info("player migration state sent",
		slog.String("guild_id", migrate.GuildID.String()),
		slog.String("url", snapshot.URL),
	)
}

func (s *Server) handlePlayerMigrateCommit(client *Client, data json.RawMessage) {
	var commit protocol.PlayerMigrateData
	if err := json.Unmarshal(data, &commit); err != nil {
		s.logger.Error("failed to unmarshal player migrate commit", slog.Any("error", err))
		return
	}

	committed := false
	var snapshot protocol.MigrateReadyData

	client.withGuildLock(commit.GuildID, func() {
		player := client.getPlayer(commit.GuildID)
		if player == nil || !player.IsMigrating() {
			return
		}

		if data, ok := player.MigrationData(); ok {
			snapshot = data
		}
		committed = true

		// Suppress every late lifecycle callback for this guild, then detach
		// the player and silently close the old voice connection.
		client.addHandoff(commit.GuildID, snapshot)
		client.removePlayer(commit.GuildID)
	})

	if !committed {
		return
	}

	s.logger.Info("player migration committed", slog.String("guild_id", commit.GuildID.String()))

	if err := s.voiceManager.CommitMigration(client.sessionID, commit.GuildID); err != nil {
		s.logger.Error("failed to close migrated voice connection", slog.Any("error", err))
	}
}

func (s *Server) handlePlayerMigrateAbort(client *Client, data json.RawMessage) {
	var abort protocol.PlayerMigrateData
	if err := json.Unmarshal(data, &abort); err != nil {
		s.logger.Error("failed to unmarshal player migrate abort", slog.Any("error", err))
		return
	}

	if _, ok := s.AbortMigration(client.sessionID, abort.GuildID); !ok {
		return
	}

	s.logger.Info("player migration aborted, resuming playback", slog.String("guild_id", abort.GuildID.String()))
}

// AbortMigration cancels a pending migration handoff for a session/guild and
// restores the player, returning the restored state. It is the programmatic
// counterpart of OpPlayerMigrateAbort, used when drain is cancelled or the
// target node reports failure. ok is false when no handoff was pending.
func (s *Server) AbortMigration(sessionID string, guildID snowflake.ID) (state string, ok bool) {
	client := s.getClientBySession(sessionID)
	if client == nil {
		return "", false
	}

	var restoredState string
	aborted := false

	client.withGuildLock(guildID, func() {
		player := client.getPlayer(guildID)
		if player == nil || !player.IsMigrating() {
			return
		}

		s.voiceManager.EndMigration(sessionID, guildID)
		player.EndMigration()
		restoredState = player.GetState()
		aborted = true
	})

	if !aborted {
		return "", false
	}

	client.send(protocol.Message{
		Op: protocol.OpPlayerUpdate,
		Data: protocol.PlayerUpdateData{
			GuildID: guildID,
			State:   restoredState,
		},
	})

	return restoredState, true
}

func (s *Server) GetStats() protocol.StatsData {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	var totalPlayers, playingTracks int
	for _, client := range s.clients {
		client.playersMu.RLock()
		totalPlayers += len(client.players)
		for _, p := range client.players {
			if p.GetState() == protocol.PlayerStatePlaying {
				playingTracks++
			}
		}
		client.playersMu.RUnlock()
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return protocol.StatsData{
		Players:       totalPlayers,
		PlayingTracks: playingTracks,
		Uptime:        time.Since(s.startTime).Milliseconds(),
		Memory:        m.Alloc,
	}
}

func (s *Server) IsDraining() bool {
	s.drainMu.RLock()
	defer s.drainMu.RUnlock()
	return s.draining
}

func (s *Server) Drain(reason string, deadlineMs int64) {
	// Wait for admissions that already passed the draining check to finish
	// mounting, so no play is left half-started across the drain transition.
	s.admitMu.Lock()
	s.drainMu.Lock()
	s.draining = true
	s.drainMu.Unlock()
	s.admitMu.Unlock()

	s.logger.Info("entering drain mode", slog.String("reason", reason), slog.Int64("deadline_ms", deadlineMs))

	s.broadcast(protocol.Message{
		Op: protocol.OpNodeDraining,
		Data: protocol.NodeDrainingData{
			Reason:     reason,
			DeadlineMs: deadlineMs,
		},
	})
}

// Undrain cancels drain mode and restores the node to service. Any migration
// handoff still in flight is aborted so those players resume playback on this
// node from their frozen (non-regressing) position.
func (s *Server) Undrain() {
	s.drainMu.Lock()
	wasDraining := s.draining
	s.draining = false
	s.drainMu.Unlock()

	if !wasDraining {
		return
	}

	s.logger.Info("drain cancelled, returning to service")

	for _, client := range s.snapshotClients() {
		client.abortAllMigrations()
	}
}

// CancelDrain is an alias for Undrain.
func (s *Server) CancelDrain() {
	s.Undrain()
}

// SetDraining flips the drain flag programmatically. Setting false cancels
// drain exactly like Undrain.
func (s *Server) SetDraining(draining bool) {
	if draining {
		s.drainMu.Lock()
		s.draining = true
		s.drainMu.Unlock()
		return
	}

	s.Undrain()
}

func (s *Server) snapshotClients() []*Client {
	s.clientsMu.RLock()
	clients := make([]*Client, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	s.clientsMu.RUnlock()
	return clients
}

func (s *Server) broadcast(message protocol.Message) {
	for _, client := range s.snapshotClients() {
		client.send(message)
	}
}

// abortAllMigrations rolls back every in-flight handoff for a client after
// drain is cancelled. AbortMigration emits the playerUpdate per player.
func (c *Client) abortAllMigrations() {
	c.playersMu.RLock()
	guildIDs := make([]snowflake.ID, 0, len(c.players))
	for guildID, player := range c.players {
		if player.IsMigrating() {
			guildIDs = append(guildIDs, guildID)
		}
	}
	c.playersMu.RUnlock()

	for _, guildID := range guildIDs {
		c.server.AbortMigration(c.sessionID, guildID)
	}
}

func (s *Server) PlayerCount() int {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	var total int
	for _, client := range s.clients {
		client.playersMu.RLock()
		total += len(client.players)
		client.playersMu.RUnlock()
	}
	return total
}

// DrainPendingCount counts players that have not started a migration
// handshake yet. A player with a served snapshot is already leaving this
// node, so drain convergence does not have to wait for an optional commit.
func (s *Server) DrainPendingCount() int {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	var total int
	for _, client := range s.clients {
		client.playersMu.RLock()
		for _, player := range client.players {
			if !player.IsMigrating() {
				total++
			}
		}
		client.playersMu.RUnlock()
	}
	return total
}
