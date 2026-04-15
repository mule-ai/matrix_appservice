// pi-matrix - A Matrix appservice for pi sessions via RPC mode.
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

package matrix

import (
	"sync"
	"time"

	"maunium.net/go/mautrix/id"
)

// Room represents a Matrix room associated with a pi session.
type Room struct {
	// ID is the Matrix room ID
	ID id.RoomID

	// Name is the room name
	Name string

	// Topic is the room topic
	Topic string

	// SessionDir is the directory path of the pi session
	SessionDir string

	// CreatedAt is when the room was created
	CreatedAt time.Time

	// LastActivity is the last time there was activity in the room
	LastActivity time.Time

	// mu protects the room fields
	mu sync.RWMutex
}

// NewRoom creates a new room.
func NewRoom(id id.RoomID, sessionDir string, name string) *Room {
	now := time.Now()
	return &Room{
		ID:           id,
		SessionDir:   sessionDir,
		Name:         name,
		CreatedAt:    now,
		LastActivity: now,
	}
}

// UpdateActivity updates the last activity timestamp.
func (r *Room) UpdateActivity() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.LastActivity = time.Now()
}

// RoomInfo returns a summary of the room.
func (r *Room) RoomInfo() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return map[string]interface{}{
		"id":            r.ID,
		"name":          r.Name,
		"topic":         r.Topic,
		"session_dir":   r.SessionDir,
		"created_at":    r.CreatedAt,
		"last_activity": r.LastActivity,
	}
}
