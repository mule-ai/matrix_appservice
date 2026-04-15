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

// TypingState tracks typing users per room.
type TypingState struct {
	users map[id.RoomID]map[id.UserID]time.Time
	mu    sync.RWMutex
}

// NewTypingState creates a new typing state tracker.
func NewTypingState() *TypingState {
	return &TypingState{
		users: make(map[id.RoomID]map[id.UserID]time.Time),
	}
}

// SetTyping sets a user as typing in a room.
func (ts *TypingState) SetTyping(roomID id.RoomID, userID id.UserID, timeout time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.users[roomID] == nil {
		ts.users[roomID] = make(map[id.UserID]time.Time)
	}
	ts.users[roomID][userID] = time.Now().Add(timeout)
}

// ClearTyping clears a user's typing state.
func (ts *TypingState) ClearTyping(roomID id.RoomID, userID id.UserID) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.users[roomID] != nil {
		delete(ts.users[roomID], userID)
		if len(ts.users[roomID]) == 0 {
			delete(ts.users, roomID)
		}
	}
}

// GetTypingUsers returns users currently typing in a room.
func (ts *TypingState) GetTypingUsers(roomID id.RoomID) []id.UserID {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	now := time.Now()
	var result []id.UserID

	for userID, expiresAt := range ts.users[roomID] {
		if expiresAt.After(now) {
			result = append(result, userID)
		}
	}
	return result
}

// IsTyping checks if a user is currently typing in a room.
func (ts *TypingState) IsTyping(roomID id.RoomID, userID id.UserID) bool {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	expiresAt, ok := ts.users[roomID][userID]
	if !ok {
		return false
	}
	return expiresAt.After(time.Now())
}

// Cleanup removes expired typing states.
func (ts *TypingState) Cleanup() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	now := time.Now()
	for roomID, users := range ts.users {
		for userID, expiresAt := range users {
			if expiresAt.Before(now) {
				delete(users, userID)
			}
		}
		if len(users) == 0 {
			delete(ts.users, roomID)
		}
	}
}
