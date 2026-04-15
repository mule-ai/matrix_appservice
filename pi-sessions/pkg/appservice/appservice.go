// pi-matrix - A Matrix appservice for pi sessions.
// Routes Matrix events to the Pi Session Manager and vice versa.
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

package appservice

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/pi-matrix/pkg/config"
	"go.mau.fi/pi-matrix/pkg/matrix"
	"go.mau.fi/pi-matrix/pkg/sessionmanager"
	"go.mau.fi/pi-matrix/pkg/store"
)

// AppService handles Matrix events and routes them to/from the Session Manager.
type AppService struct {
	config   config.Config
	mxClient *matrix.Client
	sessClient *sessionmanager.Client
	store    *store.Store

	// Track DM rooms for users
	dmRooms map[string]id.RoomID

	// Track control rooms (first room user invites bot to)
	controlRooms map[string]id.RoomID // userID -> roomID

	// Track session rooms
	sessionRooms map[string]id.RoomID

	// Active sessions per user
	userSessions map[string]string // userID -> sessionID

	logger zerolog.Logger
}

// NewAppService creates a new appservice handler.
func NewAppService(
	cfg config.Config,
	mxClient *matrix.Client,
	sessClient *sessionmanager.Client,
	s *store.Store,
	logger zerolog.Logger,
) *AppService {
	as := &AppService{
		config:       cfg,
		mxClient:     mxClient,
		sessClient:   sessClient,
		store:        s,
		dmRooms:      make(map[string]id.RoomID),
		controlRooms: make(map[string]id.RoomID),
		sessionRooms: make(map[string]id.RoomID),
		userSessions: make(map[string]string),
		logger:       logger,
	}

	// Restore session rooms from store if available
	if s != nil {
		as.restoreSessionRooms()
	}

	// Register event handlers
	mxClient.RegisterEventHandlers(
		as.onRoomMessage,
		as.onDMMessage,
		as.onRoomJoin,
		as.onRoomLeave,
	)

	// Register event listener from session manager
	sessClient.OnSessionEvent(as.onSessionEvent)

	return as
}

// restoreSessionRooms restores session room mappings from the store.
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

// saveSessionRoom saves a session room mapping to the store.
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

// deleteSessionRoom removes a session room mapping from the store.
func (as *AppService) deleteSessionRoom(sessionID string) {
	if as.store == nil {
		return
	}

	if err := as.store.DeletePortal(sessionID); err != nil {
		as.logger.Warn().Err(err).Msg("failed to delete portal from store")
	}
}

// onRoomMessage handles messages in session rooms.
func (as *AppService) onRoomMessage(roomID id.RoomID, sender id.UserID, content string) {
	as.logger.Info().
		Str("room_id", string(roomID)).
		Str("sender", string(sender)).
		Str("content", content).
		Msg("room message")

	if sender == as.mxClient.GetBotUserID() {
		return
	}

	// Check if this is a command - handle room-specific commands
	if strings.HasPrefix(content, "/") {
		parts := strings.SplitN(content, " ", 2)
		cmd := strings.ToLower(strings.TrimPrefix(parts[0], "/"))

		// Handle /new command to reset session
		if cmd == "new" {
			as.handleNewCommand(roomID)
			return
		}

		// Handle /help in room
		if cmd == "help" {
			ctx := context.Background()
			as.mxClient.SendNotice(ctx, roomID, "Room commands:\n/new - Reset session with clean context\n/help - Show this help")
			return
		}

		// Other commands go to general handler
		as.handleCommand(sender, roomID, content)
		return
	}

	// Find session for this room
	sessionID := ""
	for sessID, rID := range as.sessionRooms {
		if rID == roomID {
			sessionID = sessID
			break
		}
	}

	// If no session found, ignore
	if sessionID == "" {
		as.logger.Warn().Str("room_id", string(roomID)).Msg("no session for room, use /start command first")
		return
	}

	// Forward message to session manager
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := as.sessClient.SendPrompt(ctx, sessionID, content); err != nil {
		as.logger.Error().Err(err).Str("session_id", sessionID).Msg("failed to send prompt")
		// Session may have been lost (e.g., session manager restarted)
		// Clean up tracking and tell user to start a new session
		as.logger.Info().Str("session_id", sessionID).Msg("removing dead session")
		delete(as.sessionRooms, sessionID)
		as.mxClient.SendNotice(ctx, roomID, "Error: Session lost. Please use /start to create a new session.")
		return
	}
}

// onDMMessage handles direct messages to the bot.
func (as *AppService) onDMMessage(sender id.UserID, content string) {
	as.logger.Info().
		Str("sender", string(sender)).
		Str("content", content).
		Msg("DM received")

	// Get or create DM room for this user
	dmRoom, ok := as.dmRooms[string(sender)]
	if !ok {
		ctx := context.Background()
		room, err := as.mxClient.CreateDMRoom(ctx, sender)
		if err != nil {
			as.logger.Error().Err(err).Str("user_id", string(sender)).Msg("failed to create DM room")
			return
		}
		dmRoom = room
		as.dmRooms[string(sender)] = room
	}

	content = strings.TrimSpace(content)

	if strings.HasPrefix(content, "/") {
		as.handleCommand(sender, dmRoom, content)
	} else {
		as.handleStartCommand(sender, dmRoom, content)
	}
}

// handleCommand handles DM commands.
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
		as.handleStopCommand(ctx, sender, dmRoom, args)

	case "help":
		as.handleHelpCommand(ctx, sender, dmRoom)

	default:
		as.mxClient.SendNotice(ctx, dmRoom,
			fmt.Sprintf("Unknown command: %s. Send /help for available commands.", cmd))
	}
}

// handleStartCommand handles the /start command to create a new session.
// Command format: /start <machine_name> <directory> or /start <directory>
func (as *AppService) handleStartCommand(sender id.UserID, dmRoom id.RoomID, args string) {
	ctx := context.Background()

	args = strings.TrimSpace(args)
	if args == "" {
		as.mxClient.SendNotice(ctx, dmRoom, "Usage: /start <machine_name> <directory> or /start <directory>\n\nExample: /start desktop /data/jbutler/git/project\nExample: /start laptop ~/work")
		return
	}

	var machineName, directory string

	// Parse: /start <machine_name> <directory> or /start <directory>
	parts := strings.SplitN(args, " ", 2)
	if len(parts) == 2 {
		machineName = strings.TrimSpace(parts[0])
		directory = strings.TrimSpace(parts[1])
	} else {
		// Only directory provided, use default machine
		machineName = ""
		directory = parts[0]
	}

	// Expand home directory in path
	if strings.HasPrefix(directory, "~/") {
		home, _ := os.UserHomeDir()
		directory = filepath.Join(home, directory[2:])
	}

	// Get absolute path
	absPath, err := filepath.Abs(directory)
	if err != nil {
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Error: Invalid path: %v", err))
		return
	}

	// Check if machine name is valid
	availableManagers := as.sessClient.GetAvailableManagers()
	if machineName != "" {
		valid := false
		for _, m := range availableManagers {
			if m == machineName {
				valid = true
				break
			}
		}
		if !valid {
			as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Unknown machine: %s\nAvailable machines: %s", machineName, strings.Join(availableManagers, ", ")))
			return
		}
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Starting session on %s in %s...", machineName, absPath))
	} else {
		machineName = availableManagers[0] // Use first/default manager
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Starting session in %s...", absPath))
	}

	// Create session via session manager
	sessionID, err := as.sessClient.CreateSession(ctx, machineName, absPath, string(sender))
	if err != nil {
		as.logger.Error().Err(err).Str("machine", machineName).Str("path", absPath).Msg("failed to create session")
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Error: %v", err))
		return
	}

	// Create session room and invite user
	room, err := as.mxClient.CreateSessionRoomWithMachine(ctx, machineName, absPath, sender)
	if err != nil {
		as.logger.Error().Err(err).Str("directory", absPath).Msg("failed to create session room")
		as.mxClient.SendNotice(ctx, dmRoom, "Error: Failed to create session room")
		as.sessClient.DeleteSession(context.Background(), sessionID)
		return
	}

	// Track the session
	as.sessionRooms[sessionID] = room.ID
	as.userSessions[string(sender)] = sessionID
	as.saveSessionRoom(sessionID, room.ID)

	// Send messages
	as.mxClient.SendNotice(ctx, room.ID, fmt.Sprintf("Welcome! Session started on %s in %s", machineName, absPath))
	as.mxClient.SendNotice(ctx, dmRoom,
		fmt.Sprintf("Session started on %s! Join the room: %s", machineName, room.ID))

	as.logger.Info().
		Str("session_id", sessionID).
		Str("machine", machineName).
		Str("directory", absPath).
		Str("room_id", string(dmRoom)).
		Str("user_id", string(sender)).
		Msg("new session created")
}

// handleListCommand handles the /list command.
func (as *AppService) handleListCommand(ctx context.Context, sender id.UserID, dmRoom id.RoomID) {
	sessions, err := as.sessClient.ListSessions(ctx)
	if err != nil {
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Error: %v", err))
		return
	}

	if len(sessions) == 0 {
		as.mxClient.SendNotice(ctx, dmRoom, "No active sessions. Send /start <path> to create one.")
		return
	}

	var lines []string
	lines = append(lines, "Active sessions:")
	for _, s := range sessions {
		lines = append(lines, fmt.Sprintf("  • %s on %s (user: %s)", s.Directory, s.MachineName, s.UserID))
	}

	as.mxClient.SendNotice(ctx, dmRoom, strings.Join(lines, "\n"))
}

// handleStopCommand handles the /stop command.
func (as *AppService) handleStopCommand(ctx context.Context, sender id.UserID, dmRoom id.RoomID, path string) {
	sessionID, ok := as.userSessions[string(sender)]
	if !ok {
		as.mxClient.SendNotice(ctx, dmRoom, "No active session to stop. Use /start <path> to create one.")
		return
	}

	if err := as.sessClient.DeleteSession(ctx, sessionID); err != nil {
		as.mxClient.SendNotice(ctx, dmRoom, fmt.Sprintf("Error stopping session: %v", err))
		return
	}

	// Clean up tracking
	if roomID, ok := as.sessionRooms[sessionID]; ok {
		delete(as.sessionRooms, sessionID)
		// Note: Room cleanup handled by appservice logic
		as.mxClient.SendNotice(ctx, roomID, "Session has ended.")
	}
	delete(as.userSessions, string(sender))

	as.mxClient.SendNotice(ctx, dmRoom, "Session stopped.")
}

// handleHelpCommand handles the /help command.
func (as *AppService) handleHelpCommand(ctx context.Context, sender id.UserID, dmRoom id.RoomID) {
	availableManagers := as.sessClient.GetAvailableManagers()
	managerList := ""
	if len(availableManagers) > 0 {
		managerList = "\nAvailable machines: " + strings.Join(availableManagers, ", ")
	}
	
	help := fmt.Sprintf(`Pi Matrix Bot Commands:

/start <machine> <path> - Start a new pi session on the specified machine
/start <path>           - Start a new pi session (uses first available machine)
                         Examples:
                           /start desktop /data/jbutler/git/project
                           /start laptop ~/work
                           /start /data/jbutler/git/mule-ai%[1]s

/list   - List all active sessions (shows machine, directory, user)

/stop   - Stop your active session

/help   - Show this help message

To interact with a session, join its Matrix room. Messages will be forwarded to pi.`, managerList)

	as.mxClient.SendNotice(ctx, dmRoom, help)
}

// handleNewCommand handles the /new command in a session room to reset the session.
func (as *AppService) handleNewCommand(roomID id.RoomID) {
	ctx := context.Background()

	// Find current session for this room
	var oldSessionID string
	for sessID, rID := range as.sessionRooms {
		if rID == roomID {
			oldSessionID = sessID
			break
		}
	}

	if oldSessionID == "" {
		as.mxClient.SendNotice(ctx, roomID, "No session found for this room. Use /start <path> to create one.")
		return
	}

	// Get current session info to find the directory
	session, err := as.sessClient.GetSession(ctx, oldSessionID)
	if err != nil || session == nil {
		as.mxClient.SendNotice(ctx, roomID, "Error: Could not get session info. Use /stop and /start to create a new session.")
		return
	}

	directory := session.Directory
	userID := session.UserID
	machineName := session.MachineName

	as.logger.Info().
		Str("old_session_id", oldSessionID).
		Str("machine", machineName).
		Str("directory", directory).
		Msg("resetting session with /new")

	// Delete old session
	if err := as.sessClient.DeleteSession(ctx, oldSessionID); err != nil {
		as.logger.Warn().Err(err).Str("session_id", oldSessionID).Msg("failed to delete old session")
	}

	// Create new session in same directory and machine
	newSessionID, err := as.sessClient.CreateSession(ctx, machineName, directory, userID)
	if err != nil {
		as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("Error: Failed to create new session: %v", err))
		return
	}

	// Update session-room mapping
	as.sessionRooms[oldSessionID] = "" // Clear old
	as.sessionRooms[newSessionID] = roomID
	as.saveSessionRoom(newSessionID, roomID)
	as.deleteSessionRoom(oldSessionID)

	as.mxClient.SendNotice(ctx, roomID, "Session reset. Previous context has been cleared. Start fresh!")
}

// onRoomJoin handles a user joining a room.
func (as *AppService) onRoomJoin(roomID id.RoomID, userID id.UserID) {
	as.logger.Info().
		Str("room_id", string(roomID)).
		Str("user_id", string(userID)).
		Msg("user joined room")

	// If this is the bot joining, set this as user's control room
	if userID == as.mxClient.GetBotUserID() {
		return
	}

	// Set this room as user's control room if not already set
	if _, ok := as.controlRooms[string(userID)]; !ok {
		as.controlRooms[string(userID)] = roomID
		as.logger.Info().
			Str("room_id", string(roomID)).
			Str("user_id", string(userID)).
			Msg("set as user control room")
	}

	// Find session for this room
	sessionID := ""
	for sessID, rID := range as.sessionRooms {
		if rID == roomID {
			sessionID = sessID
			break
		}
	}

	if sessionID == "" {
		ctx := context.Background()
		as.mxClient.SendNotice(ctx, roomID,
			fmt.Sprintf("Welcome! Send /start <path> to create a session."))
		return
	}

	ctx := context.Background()
	as.mxClient.SendNotice(ctx, roomID,
		fmt.Sprintf("Welcome! Send your message to interact with pi."))
}

// onRoomLeave handles a user leaving a room.
func (as *AppService) onRoomLeave(roomID id.RoomID, userID id.UserID) {
	as.logger.Info().
		Str("room_id", string(roomID)).
		Str("user_id", string(userID)).
		Msg("user left room")
}

// onSessionEvent handles events from sessions (sent by session manager).
func (as *AppService) onSessionEvent(event *sessionmanager.SessionEvent) {
	as.logger.Debug().
		Str("session_id", event.SessionID).
		Str("event_type", event.Type).
		Msg("received session event")

	roomID, ok := as.sessionRooms[event.SessionID]
	if !ok {
		return
	}

	ctx := context.Background()

	switch event.Type {
	case "typing_start":
		as.mxClient.SetTyping(ctx, roomID, true)

	case "typing_stop":
		as.mxClient.SetTyping(ctx, roomID, false)

	case "message":
		if event.Content != "" {
			as.mxClient.SendMessage(ctx, roomID, event.Content)
		}

	case "tool_start":
		as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("🔧 Running %s...", event.ToolName))

	case "tool_end":
		if event.IsError {
			as.mxClient.SendNotice(ctx, roomID, fmt.Sprintf("❌ %s failed", event.ToolName))
		}
	}
}
