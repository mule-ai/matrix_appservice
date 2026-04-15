// pi-matrix - HTTP handler for the session manager server.
// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package session

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"go.mau.fi/pi-matrix/pkg/config"
)

// httpHandler handles HTTP requests for the session manager.
type httpHandler struct {
	config  config.SessionManagerConfig
	manager *Manager

	logger zerolog.Logger

	// SSE clients for event streaming
	sseClients    map[string]chan<- *Event
	sseClientsMu  sync.RWMutex
}

// Event represents a session event to be streamed.
type Event struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Content   string `json:"content,omitempty"`
	ToolName  string `json:"tool_name,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
}

// NewHTTPHandler creates a new HTTP handler.
func NewHTTPHandler(cfg config.Config, manager *Manager, logger zerolog.Logger) *httpHandler {
	h := &httpHandler{
		config:     cfg.SessionManager,
		manager:    manager,
		logger:     logger,
		sseClients: make(map[string]chan<- *Event),
	}
	// Set the broadcaster on manager so it can use it for session events
	manager.SetBroadcaster(h.broadcaster())
	return h
}

// ServeHTTP implements http.Handler.
func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Authenticate
	if !h.authenticate(w, r) {
		return
	}

	// Route
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	switch {
	case r.Method == "GET" && path == "health":
		h.handleHealth(w, r)
	case r.Method == "GET" && path == "sessions":
		h.handleListSessions(w, r)
	case r.Method == "POST" && path == "sessions":
		h.handleCreateSession(w, r)
	case r.Method == "GET" && len(parts) == 2 && parts[0] == "sessions":
		h.handleGetSession(w, r, parts[1])
	case r.Method == "DELETE" && len(parts) == 2 && parts[0] == "sessions":
		h.handleDeleteSession(w, r, parts[1])
	case r.Method == "POST" && len(parts) == 3 && parts[0] == "sessions" && parts[2] == "prompt":
		h.handleSendPrompt(w, r, parts[1])
	case r.Method == "GET" && path == "events":
		h.handleEvents(w, r)
	default:
		http.Error(w, "Not Found", http.StatusNotFound)
	}
}

// authenticate checks the API key.
func (h *httpHandler) authenticate(w http.ResponseWriter, r *http.Request) bool {
	if h.config.APIKey == "" {
		return true
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, "Authorization required", http.StatusUnauthorized)
		return false
	}

	if authHeader != "Bearer "+h.config.APIKey {
		http.Error(w, "Invalid API key", http.StatusUnauthorized)
		return false
	}

	return true
}

// handleHealth handles health check requests.
func (h *httpHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"sessions": h.manager.Count(),
	})
}

// handleListSessions handles listing all sessions.
func (h *httpHandler) handleListSessions(w http.ResponseWriter, r *http.Request) {
	sessions := h.manager.ListSessions()

	sessionList := make([]map[string]interface{}, 0, len(sessions))
	for _, s := range sessions {
		sessionList = append(sessionList, map[string]interface{}{
			"id":        s.ID,
			"directory":  s.Directory,
			"user_id":   s.UserID,
			"state":     s.State.String(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"sessions": sessionList,
	})
}

// handleCreateSession handles creating a new session.
func (h *httpHandler) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Directory string `json:"directory"`
		UserID   string `json:"user_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Directory == "" {
		http.Error(w, "directory is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sess, err := h.manager.CreateSession(ctx, req.Directory, req.UserID, h.broadcaster())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create session: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":        sess.ID,
		"directory":  sess.Directory,
		"user_id":   sess.UserID,
		"state":     sess.State.String(),
	})
}

// handleGetSession handles getting a session by ID.
func (h *httpHandler) handleGetSession(w http.ResponseWriter, r *http.Request, sessionID string) {
	sess, ok := h.manager.GetSession(sessionID)
	if !ok {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":        sess.ID,
		"directory":  sess.Directory,
		"user_id":   sess.UserID,
		"state":     sess.State.String(),
	})
}

// handleDeleteSession handles deleting a session.
func (h *httpHandler) handleDeleteSession(w http.ResponseWriter, r *http.Request, sessionID string) {
	if err := h.manager.DeleteSession(sessionID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete session: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleSendPrompt handles sending a prompt to a session.
func (h *httpHandler) handleSendPrompt(w http.ResponseWriter, r *http.Request, sessionID string) {
	var req struct {
		Message string `json:"message"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Message == "" {
		http.Error(w, "message is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	if err := h.manager.SendPrompt(ctx, sessionID, req.Message); err != nil {
		http.Error(w, fmt.Sprintf("Failed to send prompt: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
	})
}

// handleEvents handles SSE event streaming.
func (h *httpHandler) handleEvents(w http.ResponseWriter, r *http.Request) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Create event channel
	eventCh := make(chan *Event, 100)
	clientID := uuid.New().String()

	// Register client
	h.sseClientsMu.Lock()
	h.sseClients[clientID] = eventCh
	h.sseClientsMu.Unlock()

	// Unregister on close
	defer func() {
		h.sseClientsMu.Lock()
		delete(h.sseClients, clientID)
		h.sseClientsMu.Unlock()
		close(eventCh)
	}()

	// Send keepalive periodically
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// Stream events
	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-eventCh:
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			// Write raw bytes to ensure UTF-8 encoding is preserved
			w.Write([]byte("data: "))
			w.Write(data)
			w.Write([]byte("\n\n"))
			flusher.Flush()
		case <-ticker.C:
			// Send keepalive
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// broadcaster returns a function that broadcasts events to all SSE clients.
func (h *httpHandler) broadcaster() func(*Event) {
	return func(event *Event) {
		h.sseClientsMu.RLock()
		defer h.sseClientsMu.RUnlock()

		h.logger.Debug().
			Str("type", event.Type).
			Str("session_id", event.SessionID).
			Int("clients", len(h.sseClients)).
			Msg("broadcasting event")

		for _, ch := range h.sseClients {
			select {
			case ch <- event:
			default:
				// Channel full, skip
			}
		}
	}
}
