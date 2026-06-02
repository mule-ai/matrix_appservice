// Copyright (C) 2026 mule-ai
// SPDX-License-Identifier: AGPL-3.0-or-later

package matrix

import (
	"reflect"
	"testing"

	"maunium.net/go/mautrix/id"
)

// TestIsDMRoomMultipleRoomsPerUser locks in the fix for the
// "no new room for /start" regression. The old code stored a
// single room per user (map[user]room), so when a user had
// multiple 1:1 rooms with the bot -- the seed populates all of
// them from synapse -- the map could only hold one. The
// isDMRoom lookup for the *other* rooms returned false, so
// /start in those older rooms fell through to the room-message
// handler, which bound the new session to the existing room
// instead of opening a fresh one.
//
// The new indexes (dmRoomUser / dmUserRooms) must remember
// every DM room independently, and isDMRoom must return true
// for each of them.
func TestIsDMRoomMultipleRoomsPerUser(t *testing.T) {
	c := &Client{
		dmRoomUser:  map[id.RoomID]id.UserID{},
		dmUserRooms: map[id.UserID][]id.RoomID{},
	}

	jbutler := id.UserID("@jbutler:matrix.butler.ooo")
	r1 := id.RoomID("!room1:matrix.butler.ooo")
	r2 := id.RoomID("!room2:matrix.butler.ooo")
	r3 := id.RoomID("!room3:matrix.butler.ooo")

	// Simulate three 1:1 rooms being seeded for the same user.
	addDM := func(room id.RoomID, user id.UserID) {
		c.dmRoomUser[room] = user
		c.dmUserRooms[user] = append([]id.RoomID{room}, c.dmUserRooms[user]...)
	}
	addDM(r1, jbutler)
	addDM(r2, jbutler)
	addDM(r3, jbutler)

	// All three rooms must be recognized as DM rooms.
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, r := range []id.RoomID{r1, r2, r3} {
		if !c.isDMRoom(r) {
			t.Errorf("isDMRoom(%s) = false, want true", r)
		}
	}
	// A room we never added must not be a DM.
	if c.isDMRoom("!notadded:matrix.butler.ooo") {
		t.Error("isDMRoom on a never-added room = true, want false")
	}

	// dmUserRooms must hold all three for the user, in MRU-first order.
	got := c.dmUserRooms[jbutler]
	want := []id.RoomID{r3, r2, r1} // last added is at front
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dmUserRooms[%s] = %v, want %v", jbutler, got, want)
	}

	// CreateDMRoom (if it consults dmUserRooms) must return the
	// MRU room, i.e. the last one added.
	if got[0] != r3 {
		t.Errorf("CreateDMRoom would return %s, want %s (MRU)", got[0], r3)
	}
}
