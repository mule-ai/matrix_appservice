// pi-matrix - Matrix appservice that talks to the forge REST API.
//
// The matrix appservice does not own pi subprocesses. forge is the
// single source of truth for sessions, messages, and tool calls.
// This package translates Matrix events into forge REST calls and
// translates forge's `messages` rows back into Matrix room messages.
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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/pi-matrix/pkg/config"
	"go.mau.fi/pi-matrix/pkg/forge"
	"go.mau.fi/pi-matrix/pkg/matrix"
	"go.mau.fi/pi-matrix/pkg/store"
)

// matrixClient is the slice of the matrix client the appservice
// actually uses. Defining it as an interface lets us stub it out
// in tests.
type matrixClient interface {
	GetBotUserID() id.UserID
	CreateDMRoom(ctx context.Context, userID id.UserID) (id.RoomID, error)
	CreateSessionRoom(ctx context.Context, sessionDir string, userID id.UserID) (*matrix.Room, error)
	SendMessage(ctx context.Context, roomID id.RoomID, content string) error
	SendNotice(ctx context.Context, roomID id.RoomID, content string) error
	SetTyping(ctx context.Context, roomID id.RoomID, typing bool) error
	RegisterEventHandlers(
		onRoomMessage func(roomID id.RoomID, sender id.UserID, content string),
		onDMMessage func(sender id.UserID, content string),
		onRoomJoin func(roomID id.RoomID, userID id.UserID),
		onRoomLeave func(roomID id.RoomID, userID id.UserID),
	)
}

// Ensure the real *matrix.Client satisfies our interface. This
// line is a compile-time assertion only.
var _ matrixClient = (*matrix.Client)(nil)

// AppService handles Matrix events and routes them to/from forge.
//
// One AppService per process. It is safe to use from multiple
// goroutines; all mutable state is guarded by mu or the per-map
// locks on the consumer.
type AppService struct {
	config    config.Config
	mxClient  matrixClient
	forge     *forge.Client
	consumer  *forge.EventConsumer
	store     *store.Store
	mu        sync.Mutex

	// sessionID -> roomID. We keep this map in memory; it is
	// reconstructed from the store on startup.
	sessionRooms map[string]id.RoomID

	// (userID, workingDir) -> profileID. Lets us reuse a forge
	// profile across multiple sessions in the same directory.
	// The map is a cache; the store is the source of truth.
	profileCache map[profileKey]string

	logger zerolog.Logger
}

type profileKey struct {
	UserID string
	// Source is whatever the user typed after `/start`. For a
	// local path it might be a resolved absolute path; for a git
	// URL it's the URL verbatim. The cache and SQLite store key
	// on this so e.g. `/start /tmp/foo` and
	// `/start https://github.com/foo/bar` never collide.
	Source string
}

// NewAppService wires the matrix client, forge client, and event
// consumer together. Call StartEvents to begin the consumer.
func NewAppService(
	cfg config.Config,
	mxClient matrixClient,
	forgeClient *forge.Client,
	consumer *forge.EventConsumer,
	s *store.Store,
	logger zerolog.Logger,
) *AppService {
	as := &AppService{
		config:       cfg,
		mxClient:     mxClient,
		forge:        forgeClient,
		consumer:     consumer,
		store:        s,
		sessionRooms: make(map[string]id.RoomID),
		profileCache: make(map[profileKey]string),
		logger:       logger,
	}

	if s != nil {
		as.restoreSessionRooms()
	}

	mxClient.RegisterEventHandlers(
		as.onRoomMessage,
		as.onDMMessage,
		as.onRoomJoin,
		as.onRoomLeave,
	)

	if consumer != nil {
		consumer.OnEvent(as.onSessionEvent)
	}

	return as
}

// StartEvents starts the forge event consumer in the background.
// Returns a stop function the caller invokes at shutdown.
//
// On startup, restoreSessionRooms (called from NewAppService)
// has already populated sessionRooms from the persisted portal
// store. The consumer is currently not tracking any of those
// sessions — Track has to be called explicitly per session to
// open the SSE connection to forge. We do that here, after
// consumer.Start so each new session is already wired up before
// we add more.
//
// Without this loop, the response path is silently broken after
// any appservice restart: the user can still send messages
// (onRoomMessage -> findSessionForRoom hits the restored map
// and forwards to forge), but forge's response never reaches
// the room (onSessionEvent is never called because no
// per-session goroutine is running).
func (as *AppService) StartEvents(ctx context.Context) func() {
	if as.consumer == nil {
		return func() {}
	}
	as.consumer.Start(ctx)
	as.mu.Lock()
	restored := make([]string, 0, len(as.sessionRooms))
	for sid := range as.sessionRooms {
		restored = append(restored, sid)
	}
	as.mu.Unlock()
	for _, sid := range restored {
		if err := as.consumer.Track(ctx, sid); err != nil {
			as.logger.Warn().Err(err).Str("session_id", sid).Msg("consumer track on restored session failed; will retry on next event")
		}
	}
	if len(restored) > 0 {
		as.logger.Info().Int("count", len(restored)).Msg("consumer: re-tracked restored sessions after restart")
	}
	return as.consumer.Stop
}

// restoreSessionRooms rebuilds the sessionID -> roomID map from the
// persisted portal store.
func (as *AppService) restoreSessionRooms() {
	portals, err := as.store.GetAllPortals()
	if err != nil {
		as.logger.Warn().Err(err).Msg("failed to restore portals from store")
		return
	}
	for _, p := range portals {
		as.sessionRooms[p.SessionID] = p.RoomID
		as.logger.Info().
			Str("session_id", p.SessionID).
			Str("room_id", string(p.RoomID)).
			Msg("restored session room from store")
	}
}

func (as *AppService) saveSessionRoom(sessionID string, roomID id.RoomID) {
	if as.store == nil {
		return
	}
	portal := &store.Portal{
		SessionID: sessionID,
		RoomID:    roomID,
		RoomName:  string(roomID),
	}
	if err := as.store.SavePortal(portal); err != nil {
		as.logger.Warn().Err(err).Msg("failed to save portal to store")
	}
}

func (as *AppService) deleteSessionRoom(sessionID string) {
	if as.store == nil {
		return
	}
	if err := as.store.DeletePortal(sessionID); err != nil {
		as.logger.Warn().Err(err).Msg("failed to delete portal from store")
	}
}

// ============================================
// DM Commands
// ============================================

func (as *AppService) onDMMessage(sender id.UserID, content string) {
	as.logger.Info().
		Str("sender", string(sender)).
		Str("content", content).
		Msg("DM received")

	// The first DM from a user lands in a fresh room; the matrix
	// client auto-creates that room. We just need to handle the
	// command in that room.
	ctx := context.Background()
	dmRoom, err := as.mxClient.CreateDMRoom(ctx, sender)
	if err != nil {
		as.logger.Error().Err(err).Str("user_id", string(sender)).Msg("failed to get DM room")
		return
	}

	content = strings.TrimSpace(content)
	if strings.HasPrefix(content, "/") {
		as.handleCommand(sender, dmRoom, content)
	} else {
		// Plain text in a DM is treated as `/start <text>` for
		// backwards compatibility with the v1 protocol.
		as.handleStartCommand(sender, dmRoom, content)
	}
}

func (as *AppService) handleCommand(sender id.UserID, dmRoom id.RoomID, content string) {
	parts := strings.SplitN(content, " ", 2)
	cmd := strings.ToLower(strings.TrimPrefix(parts[0], "/"))
	var args string
	if len(parts) > 1 {
		args = strings.TrimSpace(parts[1])
	}

	ctx := context.Background()

	switch cmd {
	case "start", "new":
		as.handleStartCommand(sender, dmRoom, args)
	case "list", "sessions":
		as.handleListCommand(ctx, sender, dmRoom)
	case "stop":
		as.handleStopCommand(ctx, sender, dmRoom)
	case "help":
		as.handleHelpCommand(ctx, sender, dmRoom)
	default:
		as.mxClient.SendNotice(ctx, dmRoom,
			fmt.Sprintf("Unknown command: %s. Send /help for available commands.", cmd))
	}
}

// handleStartCommand creates a forge session in the requested
// working directory and opens a Matrix room for it.
//
// Command format: /start <directory>
//
// For backwards compat with the old v1 syntax, the first token
// is dropped if it doesn't look like a path (no slashes, no tilde,
// no dot). So `/start dev /data/...` still works.
func (as *AppService) handleStartCommand(sender id.UserID, dmRoom id.RoomID, args string) {
	ctx := context.Background()

	args = extractStartPath(args)
	if args == "" {
		as.mxClient.SendNotice(ctx, dmRoom, "Usage:\n"+
			"  /start <directory>        # bind a session to a local path (cloned into a sandbox)\n"+
			"  /start <git-url>          # clone a fresh repo into the sandbox and work there\n\n"+
			"Examples:\n"+
			"  /start /data/jbutler/git/project\n"+
			"  /start ~/work\n"+
			"  /start https://github.com/foo/bar.git\n"+
			"  /start git@github.com:foo/bar.git")
		return
	}

	as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Starting session in %s...", args))

	profileID, err := as.ensureProfile(ctx, string(sender), args)
	if err != nil {
		as.logger.Error().Err(err).Str("user_id", string(sender)).Str("source", args).Msg("failed to ensure forge profile")
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Error: %v", err))
		return
	}

	titleBase := filepath.Base(args)
	if looksLikeGitURL(args) {
		titleBase = strings.TrimSuffix(filepath.Base(args), ".git")
	}
	title := fmt.Sprintf("Pi: %s", titleBase)
	sess, err := as.forge.CreateSession(ctx, profileID, &title)
	if err != nil {
		as.logger.Error().Err(err).Str("profile_id", profileID).Str("source", args).Msg("failed to create forge session")
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Error: %v", err))
		return
	}

	// The Matrix room topic is also path-derived. We pass the
	// source arg verbatim -- forge already knows the type.
	room, err := as.mxClient.CreateSessionRoom(ctx, args, sender)
	if err != nil {
		as.logger.Error().Err(err).Str("source", args).Msg("failed to create session room")
		// Best-effort cleanup: drop the orphan session.
		_ = as.forge.DeleteSession(context.Background(), sess.Session.ID)
		as.mxClient.SendNotice(ctx, dmRoom, "Error: Failed to create session room")
		return
	}

	as.mu.Lock()
	as.sessionRooms[sess.Session.ID] = room.ID
	as.mu.Unlock()
	as.saveSessionRoom(sess.Session.ID, room.ID)

	// Start the consumer on this session.
	if as.consumer != nil {
		if err := as.consumer.Track(ctx, sess.Session.ID); err != nil {
			as.logger.Warn().Err(err).Str("session_id", sess.Session.ID).Msg("poller track failed; will retry on next event")
		}
	}

	as.mxClient.SendNotice(ctx, room.ID, fmt.Sprintf("Welcome! Session started in %s", args))
	as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Session started! Join the room: %s", room.ID))

	as.logger.Info().
		Str("session_id", sess.Session.ID).
		Str("profile_id", profileID).
		Str("source", args).
		Str("room_id", string(room.ID)).
		Str("user_id", string(sender)).
		Msg("new session created")
}

// handleListCommand lists forge sessions. The matrix appservice
// doesn't track sessions per-user anymore; we just show the full
// forge list.
func (as *AppService) handleListCommand(ctx context.Context, sender id.UserID, dmRoom id.RoomID) {
	sessions, err := as.forge.ListSessions(ctx)
	if err != nil {
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Error: %v", err))
		return
	}
	if len(sessions) == 0 {
		as.mxClient.SendNotice(ctx, dmRoom, "No active sessions. Send /start <path> to create one.")
		return
	}
	lines := []string{"Active sessions:"}
	for _, s := range sessions {
		title := ""
		if s.Title != nil {
			title = *s.Title
		}
		lines = append(lines, fmt.Sprintf("  • %s — %s", s.ID, title))
	}
	as.mxClient.SendNotice(ctx, dmRoom, strings.Join(lines, "\n"))
}

// handleStopCommand deletes the user's most recent active session.
// We identify it by walking the persisted portal mapping; if the
// user has only one session room we close it, otherwise we ask
// them to use the room's `/stop` command instead.
func (as *AppService) handleStopCommand(ctx context.Context, sender id.UserID, dmRoom id.RoomID) {
	// For now: walk all session rooms the matrix client knows
	// about and delete any that the user has joined. This is a
	// rough approximation; the room-scoped /stop below is the
	// preferred path.
	as.mu.Lock()
	sessionIDs := make([]string, 0, len(as.sessionRooms))
	for sid := range as.sessionRooms {
		sessionIDs = append(sessionIDs, sid)
	}
	as.mu.Unlock()

	if len(sessionIDs) == 0 {
		as.mxClient.SendNotice(ctx, dmRoom, "No active sessions. Use /start <path> to create one.")
		return
	}

	deleted := 0
	for _, sid := range sessionIDs {
		if err := as.forge.DeleteSession(ctx, sid); err != nil {
			as.logger.Warn().Err(err).Str("session_id", sid).Msg("failed to delete session")
			continue
		}
		as.mu.Lock()
		roomID := as.sessionRooms[sid]
		delete(as.sessionRooms, sid)
		as.mu.Unlock()
		as.deleteSessionRoom(sid)
		if as.consumer != nil {
			as.consumer.Forget(sid)
		}
		as.mxClient.SendNotice(ctx, roomID, "Session has ended.")
		deleted++
	}
	as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Stopped %d session(s).", deleted))
}

func (as *AppService) handleHelpCommand(ctx context.Context, sender id.UserID, dmRoom id.RoomID) {
	help := `Pi Matrix Bot Commands:

/start <path> - Start a new pi session in the given directory
               Examples:
                 /start /data/jbutler/git/project
                 /start ~/work

/list         - List all active sessions

/stop         - Stop your active session(s)

/help         - Show this help message

To interact with a session, join its Matrix room. Messages will be
forwarded to forge, and the agent's response will appear in the
room.

Room commands (sent inside a session room):
/new          - Reset the session with a fresh context
/steer <msg>  - Send a steering message
/help         - Show room commands
`
	as.mxClient.SendNotice(ctx, dmRoom, help)
}

// ============================================
// Room Messages
// ============================================

func (as *AppService) onRoomMessage(roomID id.RoomID, sender id.UserID, content string) {
	as.logger.Info().
		Str("room_id", string(roomID)).
		Str("sender", string(sender)).
		Str("content", content).
		Msg("room message")

	if sender == as.mxClient.GetBotUserID() {
		return
	}

	if strings.HasPrefix(content, "/") {
		as.handleRoomCommand(sender, roomID, content)
		return
	}

	// Plain message: forward to the forge session attached to
	// this room.
	sessionID := as.findSessionForRoom(roomID)
	if sessionID == "" {
		as.logger.Warn().Str("room_id", string(roomID)).Msg("no session for room, use /start first")
		ctx := context.Background()
		as.mxClient.SendNotice(ctx, roomID, "No active session for this room. Send /start <path> in a DM with me to create one (e.g. /start /data/jbutler/git/project).")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := as.forge.SendMessage(ctx, sessionID, content); err != nil {
		as.logger.Error().Err(err).Str("session_id", sessionID).Msg("failed to send prompt to forge")
		as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Error: %v", err))
	}
}

func (as *AppService) handleRoomCommand(sender id.UserID, roomID id.RoomID, content string) {
	parts := strings.SplitN(content, " ", 2)
	cmd := strings.ToLower(strings.TrimPrefix(parts[0], "/"))
	args := ""
	if len(parts) > 1 {
		args = strings.TrimSpace(parts[1])
	}
	ctx := context.Background()

	switch cmd {
	case "new":
		as.handleNewCommand(ctx, roomID)
	case "start":
		// `/start <path>` from inside a session room creates a
		// fresh forge session in <path> and binds it to this
		// room. This handles the case where the user is in a
		// pre-existing room (e.g. a DM with the bridge bot that
		// the cold-started binary doesn't know about) and wants
		// to attach a session to it.
		as.handleStartInRoomCommand(ctx, sender, roomID, args)
	case "steer":
		as.handleSteerCommand(ctx, roomID, content)
	case "help":
		as.mxClient.SendNotice(ctx, roomID, "Room commands:\n/new - Reset session with a fresh context\n/start <path> - Start a session in <path> bound to this room\n/steer <msg> - Send a steering message\n/help - Show this help")
	default:
		// Unknown command in a room; treat as a normal message
		// so the user doesn't lose their input.
		sessionID := as.findSessionForRoom(roomID)
		if sessionID == "" {
			as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Unknown command: %s", cmd))
			return
		}
		if err := as.forge.SendMessage(ctx, sessionID, content); err != nil {
			as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Error: %v", err))
		}
	}
}

// handleNewCommand resets a session by deleting the old forge
// session and creating a new one against the same profile (so the
// working dir is preserved). The Matrix room stays the same.
func (as *AppService) handleNewCommand(ctx context.Context, roomID id.RoomID) {
	as.mu.Lock()
	oldSessionID := ""
	for sid, rid := range as.sessionRooms {
		if rid == roomID {
			oldSessionID = sid
			break
		}
	}
	as.mu.Unlock()

	if oldSessionID == "" {
		as.mxClient.SendNotice(ctx, roomID, "No session for this room. Use /start <path> first.")
		return
	}

	// We need the profile_id attached to the old session. Fetch
	// it; if that fails the session is gone anyway, so just
	// inform the user.
	oldSess, err := as.forge.GetSession(ctx, oldSessionID)
	if err != nil || oldSess == nil || oldSess.ProfileID == "" {
		// We have a room binding but the old session is
		// unreadable or has no profile. Don't touch the
		// existing mapping (the session is still in forge's
		// eyes, even if the response was malformed); just
		// tell the user to /start fresh.
		as.mxClient.SendNotice(ctx, roomID, "Could not read the existing session's profile. Use /start <path-or-url> to start a new one.")
		return
	}

	// Order matters: create the new session first, then delete
	// the old. If the create fails, the old session is still
	// live in forge and the room's binding is intact, so the
	// user can retry /new or just keep using the old session.
	// The previous order (delete-then-create) could leave the
	// user with a deleted session and a room that has no
	// portal entry, which is the state this user was in
	// after a buggy /new: forge rejected the new session with
	// `profile_id: UUID parsing failed: invalid length: 0` and
	// the room was empty.
	title := ""
	if oldSess.Title != nil {
		title = *oldSess.Title
	}
	newSess, err := as.forge.CreateSession(ctx, oldSess.ProfileID, &title)
	if err != nil {
		as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Error creating new session: %v", err))
		return
	}

	// New session is up. Now retire the old.
	if err := as.forge.DeleteSession(ctx, oldSessionID); err != nil {
		as.logger.Warn().Err(err).Str("session_id", oldSessionID).Msg("failed to delete old session on /new (new session is already live)")
	}
	as.mu.Lock()
	delete(as.sessionRooms, oldSessionID)
	as.mu.Unlock()
	as.deleteSessionRoom(oldSessionID)
	if as.consumer != nil {
		as.consumer.Forget(oldSessionID)
	}

	as.mu.Lock()
	as.sessionRooms[newSess.Session.ID] = roomID
	as.mu.Unlock()
	as.saveSessionRoom(newSess.Session.ID, roomID)
	if as.consumer != nil {
		_ = as.consumer.Track(ctx, newSess.Session.ID)
	}
	as.mxClient.SendNotice(ctx, roomID, "Session reset. Previous context has been cleared. Start fresh!")
}

// handleStartInRoomCommand binds a fresh forge session to the
// given room. Used when the user is already in a room (typically
// a DM with the bridge bot that existed before the appservice
// started) and types `/start <path-or-url>` to attach a session
// to that room.
func (as *AppService) handleStartInRoomCommand(ctx context.Context, sender id.UserID, roomID id.RoomID, args string) {
	args = extractStartPath(args)
	if args == "" {
		as.mxClient.SendNotice(ctx, roomID, "Usage:\n"+
			"  /start <directory>        # bind a session to a local path (cloned into a sandbox)\n"+
			"  /start <git-url>          # clone a fresh repo into the sandbox and work there")
		return
	}

	// Reject if a session is already bound to this room.
	as.mu.Lock()
	existing := ""
	for sid, rid := range as.sessionRooms {
		if rid == roomID {
			existing = sid
			break
		}
	}
	as.mu.Unlock()
	if existing != "" {
		as.mxClient.SendNotice(ctx, roomID, "A session is already bound to this room. Use /new to reset it, or /steer <msg> to send a message.")
		return
	}

	as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Starting session in %s...", args))

	profileID, err := as.ensureProfile(ctx, string(sender), args)
	if err != nil {
		as.logger.Error().Err(err).Str("user_id", string(sender)).Str("source", args).Msg("failed to ensure forge profile")
		as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Error: %v", err))
		return
	}

	// The room title uses the basename for a path or the host
	// component for a URL. Either is more readable than the
	// raw full string.
	titleBase := filepath.Base(args)
	if looksLikeGitURL(args) {
		// Strip trailing ".git" and any path prefix to get
		// the repo name, e.g. "github.com/foo/bar" -> "bar".
		titleBase = strings.TrimSuffix(filepath.Base(args), ".git")
	}
	title := fmt.Sprintf("Pi: %s", titleBase)
	sess, err := as.forge.CreateSession(ctx, profileID, &title)
	if err != nil {
		as.logger.Error().Err(err).Str("profile_id", profileID).Str("source", args).Msg("failed to create forge session")
		as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Error: %v", err))
		return
	}

	as.mu.Lock()
	as.sessionRooms[sess.Session.ID] = roomID
	as.mu.Unlock()
	as.saveSessionRoom(sess.Session.ID, roomID)

	if as.consumer != nil {
		if err := as.consumer.Track(ctx, sess.Session.ID); err != nil {
			as.logger.Warn().Err(err).Str("session_id", sess.Session.ID).Msg("consumer track failed; will retry on next event")
		}
	}

	as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Welcome! Session started in %s", args))
	as.logger.Info().
		Str("session_id", sess.Session.ID).
		Str("profile_id", profileID).
		Str("source", args).
		Str("room_id", string(roomID)).
		Str("user_id", string(sender)).
		Msg("new session bound to existing room")
}

// handleSteerCommand sends a steering message into the active
// session. forge has no dedicated "abort current task" verb, so we
// just send another user message; the LLM will see it appended to
// the context.
func (as *AppService) handleSteerCommand(ctx context.Context, roomID id.RoomID, content string) {
	message := strings.TrimSpace(strings.TrimPrefix(content, "/steer"))
	if message == "" {
		as.mxClient.SendNotice(ctx, roomID, "Usage: /steer <message>\nSends your message to the agent.")
		return
	}
	sessionID := as.findSessionForRoom(roomID)
	if sessionID == "" {
		as.mxClient.SendNotice(ctx, roomID, "No session for this room. Use /start <path> first.")
		return
	}
	if err := as.forge.SendMessage(ctx, sessionID, message); err != nil {
		as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Error steering: %v", err))
		return
	}
	as.mxClient.SendNotice(ctx, roomID, "Message sent.")
}

// onRoomJoin / onRoomLeave are no-ops beyond logging; forge doesn't
// care about membership changes.
func (as *AppService) onRoomJoin(roomID id.RoomID, userID id.UserID) {
	as.logger.Debug().Str("room_id", string(roomID)).Str("user_id", string(userID)).Msg("user joined")
}

func (as *AppService) onRoomLeave(roomID id.RoomID, userID id.UserID) {
	as.logger.Debug().Str("room_id", string(roomID)).Str("user_id", string(userID)).Msg("user left")
}

// ============================================
// Forge event handler
// ============================================

// onSessionEvent is invoked by the consumer for every forge row.
// We translate the event into a Matrix room action.
func (as *AppService) onSessionEvent(event forge.SessionEvent) {
	as.logger.Debug().
		Str("session_id", event.SessionID).
		Str("event_type", event.Type).
		Msg("received forge event")

	roomID := as.findRoomForSession(event.SessionID)
	if roomID == "" {
		as.logger.Debug().Str("session_id", event.SessionID).Msg("no room for session event")
		return
	}

	ctx := context.Background()
	switch event.Type {
	case forge.EventTypingStart:
		_ = as.mxClient.SetTyping(ctx, roomID, true)
	case forge.EventTypingStop:
		_ = as.mxClient.SetTyping(ctx, roomID, false)
	case forge.EventMessage:
		if event.Content != "" {
			if err := as.mxClient.SendMessage(ctx, roomID, event.Content); err != nil {
				as.logger.Warn().Err(err).Str("room_id", string(roomID)).Msg("failed to send message")
			}
		}
	case forge.EventToolStart:
		as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("🔧 Running %s...", event.ToolName))
	case forge.EventToolEnd:
		if event.IsError {
			as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("❌ %s failed", event.ToolName))
		}
	}
}

// ============================================
// Helpers
// ============================================

// findSessionForRoom looks up the forge session id for a Matrix
// room. Returns "" if not tracked.
func (as *AppService) findSessionForRoom(roomID id.RoomID) string {
	as.mu.Lock()
	defer as.mu.Unlock()
	for sid, rid := range as.sessionRooms {
		if rid == roomID {
			return sid
		}
	}
	return ""
}

// findRoomForSession is the inverse of findSessionForRoom.
func (as *AppService) findRoomForSession(sessionID string) id.RoomID {
	as.mu.Lock()
	defer as.mu.Unlock()
	return as.sessionRooms[sessionID]
}

// ensureProfile returns the forge profile id for a working dir,
// minting one if needed. Profiles bind a working dir to a model
// configuration, so all matrix users sharing a directory share a
// profile. The (user, dir) -> profile_id mapping is cached in
// the SQLite store; the appservice process also keeps an
// in-memory map for fast lookups.
//
// The profile is created from the default_profile template in the
// appservice config. Operators can change the template by editing
// the YAML; the cache is keyed on (user, source) so a new
// source (a path or git URL) will pick up the latest template
// values.
func (as *AppService) ensureProfile(ctx context.Context, userID, source string) (string, error) {
	key := profileKey{UserID: userID, Source: source}

	as.mu.Lock()
	if pid, ok := as.profileCache[key]; ok {
		as.mu.Unlock()
		return pid, nil
	}
	as.mu.Unlock()

	// Check the store.
	if as.store != nil {
		cached, err := as.store.GetForgeProfile(userID, source)
		if err == nil && cached != nil {
			as.mu.Lock()
			as.profileCache[key] = cached.ProfileID
			as.mu.Unlock()
			return cached.ProfileID, nil
		}
	}

	// Resolve the source: path or git URL.
	var profile forge.Profile
	if looksLikeGitURL(source) {
		// For a git URL, the source itself doubles as
		// working_dir (forge stores it for display) and as
		// git_url (the actual clone target). forge will
		// clone the URL into a per-session sandbox
		// directory.
		profile = as.buildProfileFromTemplate(userID, source)
		gitURL := source
		profile.GitURL = &gitURL
	} else {
		absPath, err := resolveWorkingDir(source)
		if err != nil {
			return "", fmt.Errorf("resolve path: %w", err)
		}
		// Also try forge's existing-profile lookup keyed
		// on the resolved path; another process may have
		// created one. The cache key is the unresolved
		// source (so /start /tmp/foo and /start ./tmp/foo
		// are still distinct), but the forge lookup uses
		// the resolved path.
		if existing, err := as.forge.FindProfileByWorkingDir(ctx, absPath); err == nil && existing != nil {
			as.cacheAndPersistProfile(key, existing.ID, source)
			return existing.ID, nil
		}
		profile = as.buildProfileFromTemplate(userID, absPath)
	}

	// Idempotency fallback: if a profile with the same
	// name already exists (e.g. the in-memory cache and
	// the persisted store both missed, but the profile
	// was provisioned in a previous lifetime), reuse it
	// instead of POSTing and getting a 409 Conflict.
	// forge's `profiles.name` is UNIQUE, so at most one
	// match. We do this lookup for BOTH paths and git
	// URLs (the path branch's `FindProfileByWorkingDir`
	// already handles a different cache-miss case, but
	// does not cover the name case if `working_dir` was
	// resolved slightly differently on a previous run).
	profileName := profile.Name
	if existing, err := as.forge.FindProfileByName(ctx, profileName); err == nil && existing != nil {
		as.logger.Info().
			Str("user_id", userID).
			Str("source", source).
			Str("profile_id", existing.ID).
			Str("profile_name", profileName).
			Msg("ensureProfile: reusing existing profile by name (in-memory cache and store both missed; profile was provisioned in a previous lifetime)")
		as.cacheAndPersistProfile(key, existing.ID, source)
		return existing.ID, nil
	}

	created, err := as.forge.CreateProfile(ctx, profile)
	if err != nil {
		return "", fmt.Errorf("create profile: %w", err)
	}
	as.cacheAndPersistProfile(key, created.ID, source)
	return created.ID, nil
}

// buildProfileFromTemplate fills a forge.Profile from the
// default_profile template, applying the user-id+source-derived
// name and system prompt. The caller fills in WorkingDir (and
// optionally GitURL).
func (as *AppService) buildProfileFromTemplate(userID, workingDir string) forge.Profile {
	tmpl := as.config.Forge.DefaultProfile
	// Profile name must be unique. Using just the basename
	// collides when two paths share a final component
	// (e.g. `/data/jbutler/git/jbutlerdev/forge` vs
	// `/opt/pi-matrix/dev /data/jbutler/git/jbutlerdev/forge`
	// both have basename `forge`). Use the full sanitized
	// path so each working dir gets a distinct profile.
	name := fmt.Sprintf("pi-matrix-%s-%s", sanitizeForName(userID), sanitizeForName(workingDir))
	systemPrompt := tmpl.SystemPrompt
	p := forge.Profile{
		Name:         name,
		Provider:     tmpl.Provider,
		Model:        tmpl.Model,
		WorkingDir:   workingDir,
		SystemPrompt: &systemPrompt,
		Tools:        tmpl.Tools,
	}
	if tmpl.BaseURL != "" {
		s := tmpl.BaseURL
		p.BaseURL = &s
	}
	if tmpl.APIKey != "" {
		s := tmpl.APIKey
		p.APIKey = &s
	}
	return p
}

// looksLikeGitURL returns true if s looks like a git URL the
// user typed at a Matrix prompt. We accept:
//   - http://, https://, git://, ssh://
//   - git@host:owner/repo (the scp-style syntax GitHub uses)
//   - anything ending in .git
//
// Anything else is treated as a local path. This is a
// heuristic; a malicious user could type a path that begins
// with `https://` (none exist on Linux) but no real-world
// typo would trigger that.
func looksLikeGitURL(s string) bool {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(s, "http://"),
		strings.HasPrefix(s, "https://"),
		strings.HasPrefix(s, "git://"),
		strings.HasPrefix(s, "ssh://"):
		return true
	case strings.HasPrefix(s, "git@") && strings.Contains(s, ":"):
		return true
	case strings.HasSuffix(s, ".git"):
		return true
	}
	return false
}

func (as *AppService) cacheAndPersistProfile(key profileKey, profileID, workingDir string) {
	as.mu.Lock()
	as.profileCache[key] = profileID
	as.mu.Unlock()
	if as.store == nil {
		return
	}
	if err := as.store.SaveForgeProfile(&store.ForgeProfile{
		UserID:     key.UserID,
		WorkingDir: workingDir,
		ProfileID:  profileID,
		CreatedAt:  time.Now().Unix(),
	}); err != nil {
		as.logger.Warn().Err(err).Msg("failed to persist forge profile mapping")
	}
}

// extractStartPath strips a leading non-path token for backwards
// compat with the old v1 `/start <machine> <dir>` syntax. The
// first whitespace-separated token is dropped unless it already
// looks like a path (starts with `/`, `~`, or `.`).
func extractStartPath(args string) string {
	args = strings.TrimSpace(args)
	if args == "" {
		return ""
	}
	parts := strings.Fields(args)
	if len(parts) == 1 {
		return parts[0]
	}
	first := parts[0]
	if strings.HasPrefix(first, "/") || strings.HasPrefix(first, "~") || strings.HasPrefix(first, ".") {
		return first
	}
	// Drop the first token (legacy machine name) and rejoin.
	return strings.Join(parts[1:], " ")
}

// resolveWorkingDir expands `~` and turns a relative path into an
// absolute one. We don't try to create the directory; forge
// does that (or errors) when it spawns pi.
func resolveWorkingDir(p string) (string, error) {
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("could not resolve home dir: %w", err)
		}
		p = filepath.Join(home, p[2:])
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("invalid path: %w", err)
	}
	return abs, nil
}

// sanitizeForName turns an arbitrary string into something safe
// to embed in a profile name. forge's profile name column has a
// UNIQUE constraint, so we want a stable, readable value.
func sanitizeForName(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-' || r == '_':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	if len(out) == 0 {
		return "x"
	}
	return string(out)
}
