// pi-matrix - A Matrix appservice for pi sessions.
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

package store

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	_ "github.com/mattn/go-sqlite3"
	"maunium.net/go/mautrix/id"
)

// Store handles persistence for the bridge.
type Store struct {
	db     *sql.DB
	logger zerolog.Logger
	mu     sync.RWMutex
}

// Portal represents a room portal for a session.
type Portal struct {
	SessionID  string     `json:"session_id"`
	RoomID     id.RoomID  `json:"room_id"`
	RoomName   string     `json:"room_name"`
	PrimaryUser string    `json:"primary_user"`
	CreatedAt  int64      `json:"created_at"`
}

// Config stores global bridge configuration.
type Config struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NewStore creates a new store with the given database URL.
func NewStore(dbURL string, logger zerolog.Logger) (*Store, error) {
	db, err := sql.Open("sqlite3", dbURL+"?_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	s := &Store{
		db:     db,
		logger: logger,
	}

	// Initialize schema
	if err := s.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return s, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// initSchema creates the database tables if they don't exist.
func (s *Store) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS portal (
		session_id TEXT PRIMARY KEY,
		room_id TEXT NOT NULL,
		room_name TEXT NOT NULL,
		primary_user TEXT NOT NULL,
		created_at INTEGER NOT NULL
	);
	
	CREATE TABLE IF NOT EXISTS config (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);
	
	CREATE INDEX IF NOT EXISTS idx_portal_room_id ON portal(room_id);
	`

	_, err := s.db.Exec(schema)
	return err
}

// GetPortal gets a portal by session ID.
func (s *Store) GetPortal(sessionID string) (*Portal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	row := s.db.QueryRow("SELECT session_id, room_id, room_name, primary_user, created_at FROM portal WHERE session_id = ?", sessionID)
	
	var p Portal
	err := row.Scan(&p.SessionID, &p.RoomID, &p.RoomName, &p.PrimaryUser, &p.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// GetPortalByRoom gets a portal by room ID.
func (s *Store) GetPortalByRoom(roomID id.RoomID) (*Portal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	row := s.db.QueryRow("SELECT session_id, room_id, room_name, primary_user, created_at FROM portal WHERE room_id = ?", string(roomID))
	
	var p Portal
	err := row.Scan(&p.SessionID, &p.RoomID, &p.RoomName, &p.PrimaryUser, &p.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// SavePortal saves or updates a portal.
func (s *Store) SavePortal(portal *Portal) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(`
		INSERT INTO portal (session_id, room_id, room_name, primary_user, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
			room_id = excluded.room_id,
			room_name = excluded.room_name,
			primary_user = excluded.primary_user
	`, portal.SessionID, portal.RoomID, portal.RoomName, portal.PrimaryUser, portal.CreatedAt)
	
	return err
}

// DeletePortal deletes a portal by session ID.
func (s *Store) DeletePortal(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("DELETE FROM portal WHERE session_id = ?", sessionID)
	return err
}

// GetConfig gets a configuration value.
func (s *Store) GetConfig(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	row := s.db.QueryRow("SELECT value FROM config WHERE key = ?", key)
	
	var value string
	err := row.Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// SetConfig sets a configuration value.
func (s *Store) SetConfig(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(`
		INSERT INTO config (key, value)
		VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, value)
	
	return err
}

// GetPrimaryUser gets the primary user (who invited the bot).
func (s *Store) GetPrimaryUser() (string, error) {
	return s.GetConfig("primary_user")
}

// SetPrimaryUser sets the primary user.
func (s *Store) SetPrimaryUser(userID string) error {
	return s.SetConfig("primary_user", userID)
}

// GetAllPortals returns all portals.
func (s *Store) GetAllPortals() ([]*Portal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query("SELECT session_id, room_id, room_name, primary_user, created_at FROM portal")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var portals []*Portal
	for rows.Next() {
		var p Portal
		if err := rows.Scan(&p.SessionID, &p.RoomID, &p.RoomName, &p.PrimaryUser, &p.CreatedAt); err != nil {
			return nil, err
		}
		portals = append(portals, &p)
	}
	return portals, rows.Err()
}
