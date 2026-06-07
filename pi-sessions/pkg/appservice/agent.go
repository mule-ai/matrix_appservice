// pkg/appservice - Programmatic agent creation.
//
// The matrix_appservice has always supported the DM-driven flow
// (`/start <path>` creates a forge session, opens a Matrix room
// bound to it, and invites the user). The forge scheduled-agents
// system needs the same behavior, but driven by HTTP from a
// trusted operator (the forge `forge-agent-setup` script) rather
// than by a Matrix event.
//
// CreateAgent is the body of that flow, extracted so both the
// HTTP handler and the existing DM handler can share the same
// implementation. The HTTP path is the new `POST /api/v1/agents`
// endpoint; the DM path is the existing `handleStartCommand` /
// `handleStartInRoomCommand` (which now thinly wrap CreateAgent).
//
// Idempotency: if a portal already exists for the supplied
// session_id, the existing room is returned unchanged. If the
// room exists but the user is not in it, the user is re-invited.
//
// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package appservice

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"maunium.net/go/mautrix/id"

	"go.mau.fi/pi-matrix/pkg/matrix"
	"go.mau.fi/pi-matrix/pkg/store"
)

// CreateAgentRequest is the body of POST /api/v1/agents, and the
// argument to AppService.CreateAgent.
//
// ProfileID and SessionID are forge-assigned ids. The operator
// (forge-agent-setup) mints the profile and session first via
// forge's own REST API, then calls CreateAgent to bind them to
// a Matrix room. This split lets the operator see the forge-side
// result of each step before committing to the matrix side.
//
// WorkingDir is optional but recommended: it becomes the
// room's topic ("Pi session: <working_dir>") and the room
// name's basename. When the operator is creating a scheduled
// agent that has a meaningful working dir (which is most
// of them), passing it makes the room human-navigable.
//
// UserID is the Matrix user to invite. If the user is already
// a member of the room (because the operator re-ran setup
// after a previous run), the invite is a no-op.
//
// RoomName is optional; defaults to "Pi: <basename(working_dir)>"
// or "Pi: <name>" if WorkingDir is empty.
type CreateAgentRequest struct {
	ProfileID  string `json:"profile_id"`
	SessionID  string `json:"session_id"`
	WorkingDir string `json:"working_dir,omitempty"`
	UserID     string `json:"user_id"`
	RoomName   string `json:"room_name,omitempty"`
}

// CreateAgentResponse is the body of POST /api/v1/agents on
// 2xx. matrix_to_url is a `https://matrix.to/#/<room_id>`
// link that any web client can deep-link from.
type CreateAgentResponse struct {
	SessionID   string    `json:"session_id"`
	RoomID      id.RoomID `json:"room_id"`
	MatrixToURL string    `json:"matrix_to_url"`
}

// CreateAgent provisions a forge session and binds it to a
// Matrix room. Idempotent: if a portal already exists for the
// supplied session_id, the existing room is returned unchanged
// (and the user is re-invited if they were previously removed).
//
// The flow is:
//
//  1. Validate the request.
//  2. Look up the forge session by id. This is the source of
//     truth for whether the session exists; we don't trust
//     the caller blindly.
//  3. If a portal already exists for this session_id, return
//     the existing room (re-inviting the user if needed).
//  4. Verify the forge profile exists (sanity check; the
//     session already references it).
//  5. Create a Matrix room. The name and topic come from
//     WorkingDir and RoomName. The user is invited.
//  6. Persist the portal (session_id -> room_id) so the
//     consumer picks it up on the next event.
//  7. Track the session in the forge event consumer so
//     the agent's replies flow into the room.
//  8. Return the room id and a matrix.to deep link.
func (as *AppService) CreateAgent(ctx context.Context, req CreateAgentRequest) (*CreateAgentResponse, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}

	// (1) Verify the session exists. forge returns 404 if it
	// doesn't, which is the right error for the operator.
	if _, err := as.forge.GetSession(ctx, req.SessionID); err != nil {
		return nil, fmt.Errorf("forge session %q: %w", req.SessionID, err)
	}

	// (2) Idempotency: existing portal?
	if as.store != nil {
		existing, err := as.store.GetPortal(req.SessionID)
		if err != nil {
			as.logger.Warn().Err(err).Str("session_id", req.SessionID).
				Msg("CreateAgent: store lookup failed; proceeding to mint a new room")
		} else if existing != nil {
			as.logger.Info().
				Str("session_id", req.SessionID).
				Str("room_id", string(existing.RoomID)).
				Msg("CreateAgent: session already bound to a room; returning existing")

			// Re-invite the user in case they left or were
			// removed. Ignore the error: if the user is
			// already a member, InviteUser is a no-op; if
			// the homeserver is having a bad day, the
			// existing room is still useful to return.
			userID := id.UserID(req.UserID)
			if inviteErr := as.mxClient.InviteUser(ctx, existing.RoomID, userID); inviteErr != nil {
				as.logger.Warn().Err(inviteErr).
					Str("room_id", string(existing.RoomID)).
					Str("user_id", req.UserID).
					Msg("CreateAgent: re-invite on idempotent path failed (non-fatal)")
			}

			// Make sure the in-memory map and the consumer
			// are in sync. A previous process may have
			// persisted the portal but this process's
			// sessionRooms map is empty.
			as.mu.Lock()
			as.sessionRooms[req.SessionID] = existing.RoomID
			as.mu.Unlock()
			if as.consumer != nil {
				if err := as.consumer.Track(ctx, req.SessionID); err != nil {
					as.logger.Warn().Err(err).Str("session_id", req.SessionID).
						Msg("CreateAgent: idempotent path: poller track failed (non-fatal)")
				}
			}

			return &CreateAgentResponse{
				SessionID:   req.SessionID,
				RoomID:      existing.RoomID,
				MatrixToURL: matrixToURL(as.config.Homeserver.Domain, existing.RoomID),
			}, nil
		}
	}

	// (3) Sanity-check the profile. The session already
	// references it, so a 404 here would mean the session
	// was minted with a profile that has since been deleted,
	// which is a forge-side data-integrity issue.
	if _, err := as.forge.GetProfile(ctx, req.ProfileID); err != nil {
		return nil, fmt.Errorf("forge profile %q: %w", req.ProfileID, err)
	}

	// (4) Resolve the room name. WorkingDir is the operator's
	// source of truth; the explicit RoomName, if any, wins.
	roomName := req.RoomName
	if roomName == "" {
		roomName = defaultRoomName(req.WorkingDir, req.SessionID)
	}

	// (5) Create the room. We use a thin wrapper around the
	// matrix client that takes an explicit name (rather than
	// deriving one from a path), so the operator can pick
	// "Forge build-watcher" rather than "Pi: build-watcher".
	userID := id.UserID(req.UserID)
	room, err := as.createSessionRoomByName(ctx, roomName, req.WorkingDir, userID)
	if err != nil {
		return nil, fmt.Errorf("create matrix room: %w", err)
	}

	// (6) Persist the portal.
	if as.store != nil {
		portal := &store.Portal{
			SessionID:   req.SessionID,
			RoomID:      room.ID,
			RoomName:    room.Name,
			PrimaryUser: req.UserID,
		}
		if err := as.store.SavePortal(portal); err != nil {
			// Best-effort cleanup: drop the room we just
			// created so we don't leave a phantom Matrix
			// room with no forge binding. The matrix
			// client doesn't currently expose a room
			// delete; the room will be empty and the
			// operator can remove it manually. We log
			// loudly so it shows up in the journal.
			as.logger.Error().Err(err).
				Str("room_id", string(room.ID)).
				Str("session_id", req.SessionID).
				Msg("CreateAgent: save portal failed; orphan room created. " +
					"Operator should manually remove the room.")
			return nil, fmt.Errorf("save portal: %w", err)
		}
	}

	// (7) In-memory map + consumer.
	as.mu.Lock()
	as.sessionRooms[req.SessionID] = room.ID
	as.mu.Unlock()
	if as.consumer != nil {
		if err := as.consumer.Track(ctx, req.SessionID); err != nil {
			as.logger.Warn().Err(err).Str("session_id", req.SessionID).
				Msg("CreateAgent: poller track failed (non-fatal; consumer will retry on next event)")
		}
	}

	as.logger.Info().
		Str("session_id", req.SessionID).
		Str("profile_id", req.ProfileID).
		Str("room_id", string(room.ID)).
		Str("user_id", req.UserID).
		Str("working_dir", req.WorkingDir).
		Msg("agent created (HTTP path)")

	return &CreateAgentResponse{
		SessionID:   req.SessionID,
		RoomID:      room.ID,
		MatrixToURL: matrixToURL(as.config.Homeserver.Domain, room.ID),
	}, nil
}

// validate enforces the minimum required fields. Returns a
// non-nil error for any missing or malformed input. The
// error message is operator-facing; it goes back to the
// HTTP caller as the 400 body.
func (r CreateAgentRequest) validate() error {
	if strings.TrimSpace(r.SessionID) == "" {
		return fmt.Errorf("session_id is required")
	}
	if strings.TrimSpace(r.UserID) == "" {
		return fmt.Errorf("user_id is required")
	}
	// ProfileID is required for new rooms, but the
	// idempotent path doesn't actually need it (the
	// session already references a profile). Still
	// require it on the wire: callers should always
	// pass it; the idempotent path is implicit.
	if strings.TrimSpace(r.ProfileID) == "" {
		return fmt.Errorf("profile_id is required")
	}
	// MXID sanity check. The matrix_appservice validates
	// these deeper down the stack, but rejecting here
	// gives a clearer 400 than a 500 from the homeserver.
	if !looksLikeMXID(r.UserID) {
		return fmt.Errorf("user_id %q is not a valid Matrix user id (expected @localpart:domain)", r.UserID)
	}
	return nil
}

// defaultRoomName returns "Pi: <basename>" for a non-empty
// path/URL, falling back to "Pi: <first 8 chars of session>"
// when WorkingDir is empty (so the room still has a name).
func defaultRoomName(workingDir, sessionID string) string {
	if workingDir != "" {
		base := filepath.Base(workingDir)
		base = strings.TrimSuffix(base, ".git")
		return "Pi: " + base
	}
	if len(sessionID) >= 8 {
		return "Pi: " + sessionID[:8]
	}
	return "Pi: " + sessionID
}

// matrixToURL returns the canonical https://matrix.to/#/<room>
// deep link for a room id on a given homeserver domain.
func matrixToURL(domain string, roomID id.RoomID) string {
	if domain == "" {
		// Fall back to a server-less matrix.to link. The
		// homeserver domain is configured; if it's
		// missing it's a config bug, but the link is
		// still usable.
		return "https://matrix.to/#/" + url.PathEscape(string(roomID))
	}
	return "https://matrix.to/#/" + url.PathEscape(string(roomID)) + "?via=" + url.QueryEscape(domain)
}

// looksLikeMXID is a cheap shape check. The matrix client
// library has a real parser; this is just enough to reject
// obviously wrong inputs at the API boundary.
func looksLikeMXID(s string) bool {
	if len(s) < 3 || s[0] != '@' {
		return false
	}
	colon := strings.IndexByte(s, ':')
	if colon < 0 {
		return false
	}
	if colon == 1 {
		// Just "@:..." — empty localpart.
		return false
	}
	if colon == len(s)-1 {
		// "...:@:..." — empty domain.
		return false
	}
	return true
}

// createSessionRoomByName delegates to the matrix client. The
// client exposes CreateSessionRoomWithMachine, which is the
// closest existing API: it takes a machine-name string and
// builds a room name of the form "<prefix>: <machine>: <basename>".
// We use a fixed machine name "agent" and let workingDir
// supply the basename (which falls back to "session" if empty).
//
// A future matrix-client change can add a real
// CreateSessionRoomWithName(name, workingDir) that takes the
// full room name explicitly; we'll swap to that without
// changing the appservice side.
func (as *AppService) createSessionRoomByName(ctx context.Context, name, workingDir string, userID id.UserID) (*matrix.Room, error) {
	// Strip a "Pi: " prefix if the operator included it
	// in `name`; the matrix client will add it back via
	// RoomNamePrefix. This makes the API forgiving: both
	// "Forge build-watcher" and "Pi: Forge build-watcher"
	// produce the same final room name.
	name = strings.TrimPrefix(name, "Pi: ")
	return as.mxClient.CreateSessionRoomWithMachine(ctx, name, workingDir, userID)
}
