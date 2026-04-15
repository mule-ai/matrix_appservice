// pi-matrix - Session manager for pi subprocesses.
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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"go.mau.fi/pi-matrix/pkg/config"
)

// Manager handles all pi sessions.
type Manager struct {
	accumulatedText map[string]string // sessionID -> accumulated text
	sessions map[string]*Session
	mu       sync.RWMutex

	config config.SessionManagerConfig
	logger zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	// Broadcast function for session events
	broadcast func(*Event)
}

// SetBroadcaster sets the broadcast function for session events.
func (m *Manager) SetBroadcaster(broadcast func(*Event)) {
	m.broadcast = broadcast
}

// NewManager creates a new session manager.
func NewManager(ctx context.Context, config config.SessionManagerConfig, logger zerolog.Logger) *Manager {
	ctx, cancel := context.WithCancel(ctx)

	return &Manager{
		sessions:        make(map[string]*Session),
		accumulatedText: make(map[string]string),
		config:          config,
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
	}
}

// CreateSession creates a new session for the given directory.
func (m *Manager) CreateSession(ctx context.Context, directory, userID string, broadcast func(*Event)) (*Session, error) {
	absDir, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("invalid directory: %w", err)
	}

	// Check if directory exists, create if not
	info, err := os.Stat(absDir)
	if err != nil {
		if os.IsNotExist(err) {
			m.logger.Info().Str("directory", absDir).Msg("creating directory")
			if err := os.MkdirAll(absDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat directory: %w", err)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", absDir)
	}

	// Check session limit
	m.mu.RLock()
	if m.config.MaxSessions > 0 && len(m.sessions) >= m.config.MaxSessions {
		m.mu.RUnlock()
		return nil, fmt.Errorf("maximum session limit (%d) reached", m.config.MaxSessions)
	}
	m.mu.RUnlock()

	m.logger.Info().
		Str("directory", absDir).
		Str("user_id", userID).
		Msg("creating session")

	// Create session
	sess := newSession(absDir, userID, m.config, m.logger)

	// Set up event handler to broadcast events
	sess.SetEventHandler(func(s *Session, event *SessionEvent) {
		e := &Event{
			SessionID: s.ID,
		}
		switch event.Type {
		case "agent_start":
			e.Type = "typing_start"
			if broadcast != nil {
				broadcast(e)
			}
		case "agent_end":
			e.Type = "typing_stop"
			if broadcast != nil {
				broadcast(e)
			}
		case "message":
			e.Type = "message"
			e.Content = event.Text
			if broadcast != nil {
				broadcast(e)
			}
		case "text_delta":
			// Accumulate text for later emission
			m.mu.Lock()
			m.accumulatedText[s.ID] += event.Text
			m.mu.Unlock()
		case "text_end":
			// Emit accumulated text as a message
			m.mu.Lock()
			text := m.accumulatedText[s.ID]
			delete(m.accumulatedText, s.ID)
			m.mu.Unlock()
			if text != "" {
				e.Type = "message"
				e.Content = text
				if broadcast != nil {
					broadcast(e)
				}
			}
		case "tool_start":
			e.Type = "tool_start"
			e.ToolName = event.ToolName
			if broadcast != nil {
				broadcast(e)
			}
		case "tool_end":
			e.Type = "tool_end"
			e.ToolName = event.ToolName
			e.IsError = event.IsError
			if broadcast != nil {
				broadcast(e)
			}
		}
	})

	// Register
	m.mu.Lock()
	m.sessions[sess.ID] = sess
	m.mu.Unlock()

	// Start pi subprocess
	if err := sess.Start(ctx); err != nil {
		m.mu.Lock()
		delete(m.sessions, sess.ID)
		m.mu.Unlock()
		return nil, fmt.Errorf("failed to start pi: %w", err)
	}

	m.logger.Info().Str("session_id", sess.ID).Msg("session created")
	return sess, nil
}

// GetSession retrieves a session by ID.
func (m *Manager) GetSession(id string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sess, ok := m.sessions[id]
	return sess, ok
}

// DeleteSession deletes a session by ID.
func (m *Manager) DeleteSession(id string) error {
	m.mu.Lock()
	sess, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
	}
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("session not found: %s", id)
	}

	m.logger.Info().Str("session_id", id).Msg("deleting session")
	return sess.Stop()
}

// SendPrompt sends a prompt to a session.
// If the session is stopped, it restarts pi within the same session.
func (m *Manager) SendPrompt(ctx context.Context, sessionID, message string) error {
	sess, ok := m.GetSession(sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	// Restart pi if stopped (pi exits after processing in RPC mode)
	if sess.GetState() == StateStopped {
		m.logger.Info().Str("session_id", sessionID).Msg("session stopped, restarting pi")
		if err := sess.Start(ctx); err != nil {
			return fmt.Errorf("failed to restart pi: %w", err)
		}
	}

	return sess.SendPrompt(ctx, message)
}

// ListSessions returns all sessions.
func (m *Manager) ListSessions() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	return sessions
}

// Count returns the number of active sessions.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// StartCleanupTask starts the session cleanup task.
func (m *Manager) StartCleanupTask() {
	if m.config.SessionTimeout == 0 {
		return
	}

	timeout := m.config.SessionTimeoutDuration()
	if timeout == 0 {
		return
	}

	ticker := time.NewTicker(timeout / 2)
	go func() {
		for {
			select {
			case <-m.ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				m.cleanupIdleSessions()
			}
		}
	}()
}

// cleanupIdleSessions removes idle sessions.
func (m *Manager) cleanupIdleSessions() {
	timeout := m.config.SessionTimeoutDuration()
	if timeout == 0 {
		return
	}

	m.mu.RLock()
	var toStop []string
	for id, sess := range m.sessions {
		if sess.IsIdle(timeout) {
			toStop = append(toStop, id)
		}
	}
	m.mu.RUnlock()

	for _, id := range toStop {
		m.logger.Info().Str("session_id", id).Msg("removing idle session")
		m.DeleteSession(id)
	}
}

// StopAll stops all sessions and shuts down.
func (m *Manager) StopAll() {
	m.cancel()

	m.mu.RLock()
	var sessions []*Session
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.RUnlock()

	for _, s := range sessions {
		s.Stop()
	}

	m.mu.Lock()
	m.sessions = make(map[string]*Session)
	m.mu.Unlock()
}

// normalizeDir normalizes a directory path.
func normalizeDir(dir string) string {
	abs, _ := filepath.Abs(dir)
	abs = strings.TrimRight(abs, "/")
	return strings.ToLower(abs)
}
