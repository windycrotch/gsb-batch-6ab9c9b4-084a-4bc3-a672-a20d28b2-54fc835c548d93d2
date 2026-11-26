package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/disgoorg/snowflake/v2"
	"github.com/shi-gg/linkdave/server/protocol"
)

var startTime = time.Now()

// errNodeDraining is the canonical drain rejection message; tests and clients
// match on the "draining" substring.
const errNodeDraining = "node is draining, no new playback operations are accepted"

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", s.routeHealth)
	mux.HandleFunc("/stats", s.routeStats)
	mux.HandleFunc("POST /sessions/{session_id}/players/{guild_id}/play", s.withSession(s.routePlay))
	mux.HandleFunc("POST /sessions/{session_id}/players/{guild_id}/pause", s.withSession(s.routePause))
	mux.HandleFunc("POST /sessions/{session_id}/players/{guild_id}/resume", s.withSession(s.routeResume))
	mux.HandleFunc("POST /sessions/{session_id}/players/{guild_id}/stop", s.withSession(s.routeStop))
	mux.HandleFunc("POST /sessions/{session_id}/players/{guild_id}/seek", s.withSession(s.routeSeek))
	mux.HandleFunc("DELETE /sessions/{session_id}/players/{guild_id}", s.withSession(s.routeDisconnect))
}

type sessionHandler func(client *Client, guildID snowflake.ID, w http.ResponseWriter, r *http.Request)

func (s *Server) withSession(next sessionHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.password != "" && r.Header.Get("Authorization") != "Bearer "+s.password {
			writeJSON(w, http.StatusUnauthorized, protocol.ErrorResponse{Error: "Unauthorized"})
			return
		}

		sessionID := r.PathValue("session_id")
		guildIDStr := r.PathValue("guild_id")

		client := s.getClientBySession(sessionID)
		if client == nil {
			writeJSON(w, http.StatusNotFound, protocol.ErrorResponse{Error: "session not found"})
			return
		}

		select {
		case <-client.closeChan:
			writeJSON(w, http.StatusNotFound, protocol.ErrorResponse{Error: "session has been closed"})
			return
		default:
		}

		guildID, err := snowflake.Parse(guildIDStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, protocol.ErrorResponse{Error: "invalid guild_id"})
			return
		}

		next(client, guildID, w, r)
	}
}

// admitPlayback serializes a playback mutation against drain transitions. It
// returns false after writing the rejection response when the node is
// draining; otherwise the returned release must be deferred.
func (s *Server) admitPlayback(w http.ResponseWriter) (release func(), ok bool) {
	s.admitMu.RLock()
	if s.IsDraining() {
		s.admitMu.RUnlock()
		writeJSON(w, http.StatusServiceUnavailable, protocol.ErrorResponse{Error: errNodeDraining})
		return nil, false
	}

	return s.admitMu.RUnlock, true
}

func (s *Server) routeHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) routeStats(w http.ResponseWriter, _ *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	response := protocol.StatsResponse{
		Version:      s.version,
		Runtime:      runtime.Version(),
		Uptime:       time.Since(startTime).Milliseconds(),
		NumGoroutine: runtime.NumGoroutine(),
		Memory:       memStats.Alloc,
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) routePlay(client *Client, guildID snowflake.ID, w http.ResponseWriter, r *http.Request) {
	release, admitted := s.admitPlayback(w)
	if !admitted {
		return
	}
	defer release()

	var play protocol.RequestPlay
	if err := json.NewDecoder(r.Body).Decode(&play); err != nil {
		writeJSON(w, http.StatusBadRequest, protocol.ErrorResponse{Error: "invalid request body"})
		return
	}

	if err := play.Filters.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, protocol.ErrorResponse{Error: err.Error()})
		return
	}
	play.Filters = play.Filters.Normalize()

	var status int
	client.withGuildLock(guildID, func() {
		// Re-check under the guild lock: a migration handshake may have
		// removed the player while this request was waiting.
		player := client.getPlayer(guildID)
		if player == nil {
			status = http.StatusNotFound
			writeJSON(w, status, protocol.ErrorResponse{Error: "player not found"})
			return
		}

		s.logger.Info("play requested",
			slog.String("guild_id", guildID.String()),
			slog.String("url", play.URL),
		)

		source, err := s.voiceManager.Play(context.Background(), client.sessionID, guildID, play.URL, play.StartTime, play.Filters)
		if err != nil {
			s.logger.Error("playback failed", slog.Any("error", err))
			status = http.StatusInternalServerError
			writeJSON(w, status, protocol.ErrorResponse{Error: err.Error()})
			return
		}

		player.SetPlayingState(play.URL, play.StartTime, play.RequesterID, play.Filters)

		client.send(protocol.Message{
			Op: protocol.OpTrackStart,
			Data: protocol.TrackStartData{
				GuildID: guildID,
				Track: protocol.TrackInfo{
					URL:         source.URL(),
					Duration:    source.Duration(),
					RequesterID: play.RequesterID,
				},
			},
		})

		status = http.StatusNoContent
		w.WriteHeader(status)
	})
}

func (s *Server) routePause(client *Client, guildID snowflake.ID, w http.ResponseWriter, _ *http.Request) {
	release, admitted := s.admitPlayback(w)
	if !admitted {
		return
	}
	defer release()

	client.withGuildLock(guildID, func() {
		player := client.getPlayer(guildID)
		if player == nil {
			writeJSON(w, http.StatusNotFound, protocol.ErrorResponse{Error: "player not found"})
			return
		}

		if err := s.voiceManager.Pause(client.sessionID, guildID); err != nil {
			s.logger.Error("failed to pause", slog.Any("error", err))
			writeJSON(w, http.StatusInternalServerError, protocol.ErrorResponse{Error: err.Error()})
			return
		}

		player.SetPausedState(s.voiceManager.Position(client.sessionID, guildID))

		client.send(protocol.Message{
			Op: protocol.OpPlayerUpdate,
			Data: protocol.PlayerUpdateData{
				GuildID: guildID,
				State:   player.GetState(),
			},
		})

		w.WriteHeader(http.StatusNoContent)
	})
}

func (s *Server) routeResume(client *Client, guildID snowflake.ID, w http.ResponseWriter, _ *http.Request) {
	release, admitted := s.admitPlayback(w)
	if !admitted {
		return
	}
	defer release()

	client.withGuildLock(guildID, func() {
		player := client.getPlayer(guildID)
		if player == nil {
			writeJSON(w, http.StatusNotFound, protocol.ErrorResponse{Error: "player not found"})
			return
		}

		if err := s.voiceManager.Resume(client.sessionID, guildID); err != nil {
			s.logger.Error("failed to resume", slog.Any("error", err))
			writeJSON(w, http.StatusInternalServerError, protocol.ErrorResponse{Error: err.Error()})
			return
		}

		player.SetState(protocol.PlayerStatePlaying)
		player.SetStartedAt(time.Now())
		player.SetPosition(s.voiceManager.Position(client.sessionID, guildID))

		client.send(protocol.Message{
			Op: protocol.OpPlayerUpdate,
			Data: protocol.PlayerUpdateData{
				GuildID: guildID,
				State:   player.GetState(),
			},
		})

		w.WriteHeader(http.StatusNoContent)
	})
}

func (s *Server) routeStop(client *Client, guildID snowflake.ID, w http.ResponseWriter, _ *http.Request) {
	release, admitted := s.admitPlayback(w)
	if !admitted {
		return
	}
	defer release()

	client.withGuildLock(guildID, func() {
		player := client.getPlayer(guildID)
		if player == nil {
			writeJSON(w, http.StatusNotFound, protocol.ErrorResponse{Error: "player not found"})
			return
		}

		if err := s.voiceManager.Stop(client.sessionID, guildID); err != nil {
			s.logger.Error("failed to stop", slog.Any("error", err))
			writeJSON(w, http.StatusInternalServerError, protocol.ErrorResponse{Error: err.Error()})
			return
		}

		player.SetIdleState()

		client.send(protocol.Message{
			Op: protocol.OpPlayerUpdate,
			Data: protocol.PlayerUpdateData{
				GuildID: guildID,
				State:   player.GetState(),
			},
		})

		w.WriteHeader(http.StatusNoContent)
	})
}

func (s *Server) routeSeek(client *Client, guildID snowflake.ID, w http.ResponseWriter, r *http.Request) {
	release, admitted := s.admitPlayback(w)
	if !admitted {
		return
	}
	defer release()

	var seek protocol.RequestSeek
	if err := json.NewDecoder(r.Body).Decode(&seek); err != nil {
		writeJSON(w, http.StatusBadRequest, protocol.ErrorResponse{Error: "invalid request body"})
		return
	}

	client.withGuildLock(guildID, func() {
		player := client.getPlayer(guildID)
		if player == nil {
			writeJSON(w, http.StatusNotFound, protocol.ErrorResponse{Error: "player not found"})
			return
		}

		if err := s.voiceManager.Seek(client.sessionID, guildID, seek.Position); err != nil {
			s.logger.Error("failed to seek", slog.Any("error", err))

			if strings.Contains(err.Error(), "not supported") {
				writeJSON(w, http.StatusNotImplemented, protocol.ErrorResponse{Error: err.Error()})
				return
			}

			writeJSON(w, http.StatusInternalServerError, protocol.ErrorResponse{Error: err.Error()})
			return
		}

		player.SetPosition(s.voiceManager.Position(client.sessionID, guildID))
		player.SetStartedAt(time.Now())

		w.WriteHeader(http.StatusNoContent)
	})
}

func (s *Server) routeDisconnect(client *Client, guildID snowflake.ID, w http.ResponseWriter, _ *http.Request) {
	s.logger.Info("processing disconnect", slog.String("guild_id", guildID.String()))

	client.withGuildLock(guildID, func() {
		client.removeHandoff(guildID)
		client.removePlayer(guildID)

		if err := s.voiceManager.Disconnect(client.sessionID, guildID); err != nil {
			s.logger.Error("failed to disconnect", slog.Any("error", err))
		}
	})

	client.send(protocol.Message{
		Op: protocol.OpVoiceDisconnect,
		Data: protocol.VoiceDisconnectData{
			GuildID: guildID,
			Reason:  protocol.DisconnectReasonRequested,
		},
	})

	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}
