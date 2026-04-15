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

	"maunium.net/go/mautrix/id"
)

// RoomRegistry tracks the mapping between rooms and sessions.
type RoomRegistry struct {
	// rooms maps room ID to Room
	rooms map[id.RoomID]*Room

	// sessionToRoom maps session directory to room ID
	sessionToRoom map[string]id.RoomID

	// mu protects the maps
	mu sync.RWMutex
}

// NewRoomRegistry creates a new room registry.
func NewRoomRegistry() *RoomRegistry {
	return &RoomRegistry{
		rooms:         make(map[id.RoomID]*Room),
		sessionToRoom: make(map[string]id.RoomID),
	}
}

// Register registers a room with a session.
func (r *RoomRegistry) Register(room *Room) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rooms[room.ID] = room
	if room.SessionDir != "" {
		r.sessionToRoom[room.SessionDir] = room.ID
	}
}

// Unregister removes a room from the registry.
func (r *RoomRegistry) Unregister(roomID id.RoomID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	room, exists := r.rooms[roomID]
	if exists && room.SessionDir != "" {
		delete(r.sessionToRoom, room.SessionDir)
	}
	delete(r.rooms, roomID)
}

// GetRoomForSession gets the room associated with a session directory.
func (r *RoomRegistry) GetRoomForSession(sessionDir string) (*Room, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	roomID, ok := r.sessionToRoom[sessionDir]
	if !ok {
		return nil, false
	}

	room, ok := r.rooms[roomID]
	return room, ok
}

// GetSessionForRoom gets the session directory for a room.
func (r *RoomRegistry) GetSessionForRoom(roomID id.RoomID) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	room, ok := r.rooms[roomID]
	if !ok {
		return "", false
	}

	return room.SessionDir, room.SessionDir != ""
}

// Get gets a room by ID.
func (r *RoomRegistry) Get(roomID id.RoomID) (*Room, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	room, ok := r.rooms[roomID]
	return room, ok
}

// List returns all registered rooms.
func (r *RoomRegistry) List() []*Room {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rooms := make([]*Room, 0, len(r.rooms))
	for _, room := range r.rooms {
		rooms = append(rooms, room)
	}
	return rooms
}

// Count returns the number of registered rooms.
func (r *RoomRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.rooms)
}

// Clear removes all rooms from the registry.
func (r *RoomRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rooms = make(map[id.RoomID]*Room)
	r.sessionToRoom = make(map[string]id.RoomID)
}
