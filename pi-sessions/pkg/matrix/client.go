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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gomarkdown/markdown"
	"github.com/microcosm-cc/bluemonday"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/pi-matrix/pkg/config"
)

// Client handles Matrix operations for the appservice.
type Client struct {
	// config
	homeserver  config.HomeserverConfig
	appservice  config.AppserviceConfig
	bridge      config.BridgeConfig

	// mautrix
	mx *mautrix.Client
	as *appservice.AppService

	// room registry
	roomRegistry *RoomRegistry

	// DM tracking - tracks which DM rooms belong to which users
	dmRooms map[id.UserID]id.RoomID

	// primary user for bot interactions
	primaryUser id.UserID

	// Typing state
	typingState *TypingState

	// state
	mu sync.RWMutex

	// event handlers
	onRoomMessage func(roomID id.RoomID, sender id.UserID, content string)
	onDMMessage  func(sender id.UserID, content string)
	onRoomJoin  func(roomID id.RoomID, userID id.UserID)
	onRoomLeave func(roomID id.RoomID, userID id.UserID)
	onTyping    func(roomID id.RoomID, userID id.UserID, typing bool)

	// logger
	logger zerolog.Logger

	// context
	ctx    context.Context
	cancel context.CancelFunc
}

// ClientConfig contains configuration for the Matrix client.
type ClientConfig struct {
	Homeserver config.HomeserverConfig
	Appservice config.AppserviceConfig
	Bridge    config.BridgeConfig
	Logger    *zerolog.Logger
}

// NewClient creates a new Matrix client.
func NewClient(cfg ClientConfig, ctx context.Context) (*Client, error) {
	if cfg.Logger == nil {
		logger := zerolog.Nop()
		cfg.Logger = &logger
	}

	ctx, cancel := context.WithCancel(ctx)

	cl := &Client{
		homeserver:   cfg.Homeserver,
		appservice:   cfg.Appservice,
		bridge:       cfg.Bridge,
		roomRegistry: NewRoomRegistry(),
		dmRooms:      make(map[id.UserID]id.RoomID),
		typingState:  NewTypingState(),
		logger:       *cfg.Logger,
		ctx:          ctx,
		cancel:       cancel,
	}

	return cl, nil
}

// Start starts the Matrix client and appservice.
func (c *Client) Start() error {
	c.logger.Info().Msg("starting Matrix client")

	// Create appservice instance
	c.as = appservice.Create()
	c.as.HomeserverDomain = c.homeserver.Domain
	c.as.Log = c.logger

	// Set up the homeserver URL
	if err := c.as.SetHomeserverURL(c.homeserver.Address); err != nil {
		return fmt.Errorf("failed to set homeserver URL: %w", err)
	}

	// Create registration
	rateLimited := false
	c.as.Registration = &appservice.Registration{
		ID:              c.appservice.ID,
		URL:             c.appservice.URL,
		SenderLocalpart: c.appservice.Localpart,
		RateLimited:     &rateLimited,
		AppToken:        c.appservice.ASToken,
		ServerToken:     c.appservice.HSToken,
	}

	// Register namespace for session users (format: @pi-session-*:domain)
	c.as.Registration.Namespaces.UserIDs = appservice.NamespaceList{
		{Regex: fmt.Sprintf("@%s-.*:%s", c.appservice.ID, c.homeserver.Domain), Exclusive: true},
	}

	// Create the mautrix client for the bot
	botUserID := id.UserID(fmt.Sprintf("@%s:%s", c.appservice.Localpart, c.homeserver.Domain))
	c.mx = c.as.NewMautrixClient(botUserID)
	c.mx.Log = c.logger

	// Create event channel
	c.as.Events = make(chan *event.Event, 64)

	// Start the event handler loop
	go c.eventLoop()

	c.logger.Info().Msg("Matrix client started successfully")
	return nil
}

// eventLoop handles incoming events from the appservice.
func (c *Client) eventLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case evt, ok := <-c.as.Events:
			if !ok {
				return
			}
			c.handleEvent(evt)
		}
	}
}

// handleEvent handles a single Matrix event.
func (c *Client) handleEvent(evt *event.Event) {
	c.logger.Debug().
		Str("type", evt.Type.String()).
		Str("room_id", string(evt.RoomID)).
		Str("sender", string(evt.Sender)).
		Msg("received event")

	switch evt.Type {
	case event.EventMessage:
		c.handleMessage(evt)
	case event.EventEncrypted:
		c.logger.Debug().Msg("ignoring encrypted message")
	case event.StateMember:
		c.handleMemberEvent(evt)
	default:
		c.logger.Trace().Str("type", evt.Type.String()).Msg("ignoring event type")
	}
}

// handleMessage handles room messages.
func (c *Client) handleMessage(evt *event.Event) {
	content, ok := evt.Content.Parsed.(*event.MessageEventContent)
	if !ok {
		c.logger.Warn().Msg("failed to parse message content")
		return
	}

	// Ignore messages from the bot
	if evt.Sender == c.as.BotMXID() {
		return
	}

	roomID := evt.RoomID
	sender := evt.Sender

	// Check if this is a DM room
	c.mu.RLock()
	isDM := c.isDMRoom(roomID)
	c.mu.RUnlock()

	if isDM {
		c.logger.Info().
			Str("sender", string(sender)).
			Str("content", content.Body).
			Msg("received DM")
		if c.onDMMessage != nil {
			go c.onDMMessage(sender, content.Body)
		}
		return
	}

	// Regular room message
	c.logger.Info().
		Str("room_id", string(roomID)).
		Str("sender", string(sender)).
		Str("content", content.Body).
		Msg("received room message")

	if c.onRoomMessage != nil {
		go c.onRoomMessage(roomID, sender, content.Body)
	}
}

// handleMemberEvent handles member events (join/leave).
func (c *Client) handleMemberEvent(evt *event.Event) {
	content, ok := evt.Content.Parsed.(*event.MemberEventContent)
	if !ok {
		return
	}

	roomID := evt.RoomID
	stateKey := evt.StateKey
	if stateKey == nil {
		return
	}
	userIDStr := *stateKey

	switch content.Membership {
	case event.MembershipInvite:
		// Check if the bot is being invited
		if userIDStr == string(c.as.BotMXID()) {
			inviter := evt.Sender
			c.logger.Info().
				Str("room_id", string(roomID)).
				Str("inviter", string(inviter)).
				Msg("bot invited to room")

			// Set primary user if not set
			c.mu.Lock()
			if c.primaryUser == "" {
				c.primaryUser = inviter
				c.logger.Info().Str("primary_user", string(c.primaryUser)).Msg("set primary user from invite")
			}
			// Track this as a potential DM room
			c.dmRooms[inviter] = roomID
			c.mu.Unlock()

			// Auto-join the room
			go c.joinRoom(roomID)
		}

	case event.MembershipJoin:
		c.logger.Info().
			Str("room_id", string(roomID)).
			Str("user_id", userIDStr).
			Msg("user joined room")

		// If bot joined, set primary user
		if userIDStr == string(c.as.BotMXID()) {
			c.mu.Lock()
			if c.primaryUser == "" {
				// Try to find inviter from DM tracking
				for user, dmRoom := range c.dmRooms {
					if dmRoom == roomID {
						c.primaryUser = user
						c.logger.Info().Str("primary_user", string(c.primaryUser)).Msg("set primary user from DM tracking")
						break
					}
				}
			}
			c.mu.Unlock()
		}

		if c.onRoomJoin != nil {
			go c.onRoomJoin(roomID, id.UserID(userIDStr))
		}

	case event.MembershipLeave, event.MembershipBan:
		c.logger.Info().
			Str("room_id", string(roomID)).
			Str("user_id", userIDStr).
			Msg("user left room")

		if c.onRoomLeave != nil {
			go c.onRoomLeave(roomID, id.UserID(userIDStr))
		}
	}
}

// joinRoom makes the bot join a room.
func (c *Client) joinRoom(roomID id.RoomID) {
	bot := c.as.BotIntent()
	_, err := bot.JoinRoomByID(context.Background(), roomID)
	if err != nil {
		c.logger.Error().Err(err).Str("room_id", string(roomID)).Msg("failed to join room")
	} else {
		c.logger.Info().Str("room_id", string(roomID)).Msg("joined room")
	}
}

// isDMRoom checks if a room is a DM room (tracked in dmRooms).
func (c *Client) isDMRoom(roomID id.RoomID) bool {
	for _, dmRoom := range c.dmRooms {
		if dmRoom == roomID {
			return true
		}
	}
	return false
}

// CreateDMRoom creates or returns an existing DM room with a user.
func (c *Client) CreateDMRoom(ctx context.Context, userID id.UserID) (id.RoomID, error) {
	// Check if DM room already exists
	c.mu.RLock()
	if existingRoom, ok := c.dmRooms[userID]; ok {
		c.mu.RUnlock()
		return existingRoom, nil
	}
	c.mu.RUnlock()

	// Create a new DM room
	bot := c.as.BotIntent()

	resp, err := bot.CreateRoom(ctx, &mautrix.ReqCreateRoom{
		Name:    fmt.Sprintf("Pi Matrix - %s", userID),
		Topic:   "Control pi sessions via this DM",
		Preset:  "private_chat",
		Invite:  []id.UserID{userID},
		IsDirect: true,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create DM room: %w", err)
	}

	// Track the DM room
	c.mu.Lock()
	c.dmRooms[userID] = resp.RoomID
	c.mu.Unlock()

	c.logger.Info().
		Str("room_id", string(resp.RoomID)).
		Str("user_id", string(userID)).
		Msg("created DM room")

	return resp.RoomID, nil
}

// CreateSessionRoom creates a Matrix room for a pi session.
func (c *Client) CreateSessionRoom(ctx context.Context, sessionDir string, userID id.UserID) (*Room, error) {
	c.logger.Info().
		Str("directory", sessionDir).
		Str("user_id", string(userID)).
		Msg("creating session room")

	// Generate room name
	roomName := c.bridge.RoomNamePrefix + ": " + filepath.Base(sessionDir)

	// Create the room
	bot := c.as.BotIntent()

	resp, err := bot.CreateRoom(ctx, &mautrix.ReqCreateRoom{
		Name:   roomName,
		Topic:  fmt.Sprintf("Pi session: %s", sessionDir),
		Preset: "private_chat",
		Invite: []id.UserID{userID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create room: %w", err)
	}

	room := &Room{
		ID:         resp.RoomID,
		Name:       roomName,
		SessionDir: sessionDir,
		CreatedAt:  time.Now(),
	}

	// Register the room
	c.roomRegistry.Register(room)

	c.logger.Info().
		Str("room_id", string(room.ID)).
		Str("directory", sessionDir).
		Msg("session room created")

	return room, nil
}

// SendMessage sends a text message to a room.
// If the content contains markdown formatting, it will be converted to HTML.
func (c *Client) SendMessage(ctx context.Context, roomID id.RoomID, content string) error {
	// Convert markdown to HTML for better Matrix rendering
	plain, html := ConvertMarkdownToHTML(content)
	if html != "" {
		return c.SendFormattedMessage(ctx, roomID, plain, "org.matrix.custom.html", html)
	}
	return c.SendFormattedMessage(ctx, roomID, content, "", "")
}

// ConvertMarkdownToHTML converts markdown text to HTML.
// Returns plain text and HTML version.
func ConvertMarkdownToHTML(content string) (string, string) {
	// Check if content has markdown markers
	hasMarkdown := false
	if bytes.Contains([]byte(content), []byte("```")) ||
		bytes.Contains([]byte(content), []byte("`")) ||
		bytes.Contains([]byte(content), []byte("**")) ||
		bytes.Contains([]byte(content), []byte("__")) ||
		bytes.Contains([]byte(content), []byte("##")) {
		hasMarkdown = true
	}

	if !hasMarkdown {
		return content, ""
	}

	// Convert markdown to HTML
	html := markdown.ToHTML([]byte(content), nil, nil)

	// Sanitize HTML to prevent XSS (bluemonday)
	policy := bluemonday.UGCPolicy()
	sanitized := policy.Sanitize(string(html))

	// Convert newlines to <br> for display
	sanitized = string(bytes.ReplaceAll([]byte(sanitized), []byte("\n"), []byte("<br>")))

	// Create plain text version by stripping markdown markers
	plain := stripMarkdown(content)

	return plain, string(sanitized)
}

// stripMarkdown removes basic markdown formatting from text.
func stripMarkdown(content string) string {
	// Replace common markdown patterns
	result := content

	// Code blocks ```code```
	codeBlockRegex := regexp.MustCompile("```[\\s\\S]*?```|`([^`]+)`")
	result = codeBlockRegex.ReplaceAllString(result, "$1")

	// Bold **text** or __text__
	result = strings.ReplaceAll(result, "**", "")
	result = strings.ReplaceAll(result, "__", "")

	// Inline code `text`
	result = strings.ReplaceAll(result, "`", "'")

	// Headers ### text
	result = strings.ReplaceAll(result, "## ", "")

	// Remove box-drawing and special UTF-8 characters that may not render well
	result = removeSpecialChars(result)

	return result
}

// removeSpecialChars removes problematic UTF-8 characters.
func removeSpecialChars(s string) string {
	result := s

	// Replace em dashes with regular dash using Unicode code points
	result = strings.ReplaceAll(result, string(rune(0x2013)), "-") // en dash
	result = strings.ReplaceAll(result, string(rune(0x2014)), "-") // em dash
	result = strings.ReplaceAll(result, string(rune(0x2018)), "'") // left single quote
	result = strings.ReplaceAll(result, string(rune(0x2019)), "'") // right single quote
	result = strings.ReplaceAll(result, string(rune(0x201C)), "'") // left double quote
	result = strings.ReplaceAll(result, string(rune(0x201D)), "'") // right double quote
	result = strings.ReplaceAll(result, string(rune(0x2022)), "-") // bullet
	result = strings.ReplaceAll(result, string(rune(0x2192)), "->") // right arrow
	result = strings.ReplaceAll(result, string(rune(0x2190)), "<-") // left arrow

	// Common box-drawing characters and other special chars
	specialChars := []string{
		"─", "│", "├", "┤", "┌", "┐", "└", "┘",
		"═", "║", "╔", "╗", "╚", "╝", "╠", "╣",
		"━", "┃", "┏", "┓", "┗", "┛", "┣", "┫",
	}
	for _, c := range specialChars {
		result = strings.ReplaceAll(result, c, "")
	}

	// Replace multiple spaces with single space
	for strings.Contains(result, "  ") {
		result = strings.ReplaceAll(result, "  ", " ")
	}

	return strings.TrimSpace(result)
}

// SendFormattedMessage sends a formatted message to a room.
func (c *Client) SendFormattedMessage(ctx context.Context, roomID id.RoomID, content, format, htmlContent string) error {
	msgContent := event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    content,
	}

	if format == "org.matrix.custom.html" && htmlContent != "" {
		msgContent.Format = event.FormatHTML
		msgContent.FormattedBody = htmlContent
	}

	bot := c.as.BotIntent()
	_, err := bot.SendMessageEvent(ctx, roomID, event.EventMessage, msgContent)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// SendNotice sends a notice message to a room.
func (c *Client) SendNotice(ctx context.Context, roomID id.RoomID, content string) error {
	msgContent := event.MessageEventContent{
		MsgType: event.MsgNotice,
		Body:    content,
	}

	bot := c.as.BotIntent()
	_, err := bot.SendMessageEvent(ctx, roomID, event.EventMessage, msgContent)
	return err
}

// SetTyping sends a typing indicator to a room.
func (c *Client) SetTyping(ctx context.Context, roomID id.RoomID, typing bool) error {
	bot := c.as.BotIntent()

	timeout := 5 * time.Second
	if !typing {
		timeout = 0
	}

	_, err := bot.UserTyping(ctx, roomID, typing, timeout)
	if err != nil {
		c.logger.Warn().Err(err).Str("room_id", string(roomID)).Msg("failed to send typing indicator")
	}

	return err
}

// InviteUser invites a user to a room.
func (c *Client) InviteUser(ctx context.Context, roomID id.RoomID, userID id.UserID) error {
	bot := c.as.BotIntent()
	_, err := bot.InviteUser(ctx, roomID, &mautrix.ReqInviteUser{UserID: userID})
	return err
}

// DeleteRoom deletes (or leaves) a Matrix room.
func (c *Client) DeleteRoom(ctx context.Context, roomID id.RoomID) error {
	c.logger.Info().Str("room_id", string(roomID)).Msg("deleting room")

	// Unregister from registry
	c.roomRegistry.Unregister(roomID)

	// Leave the room
	bot := c.as.BotIntent()
	_, err := bot.LeaveRoom(ctx, roomID)
	if err != nil {
		c.logger.Warn().Err(err).Str("room_id", string(roomID)).Msg("failed to leave room")
	}

	return nil
}

// GetRoomForSession gets the room associated with a session directory.
func (c *Client) GetRoomForSession(sessionDir string) (*Room, bool) {
	return c.roomRegistry.GetRoomForSession(sessionDir)
}

// GetSessionForRoom gets the session directory for a room.
func (c *Client) GetSessionForRoom(roomID id.RoomID) (string, bool) {
	return c.roomRegistry.GetSessionForRoom(roomID)
}

// GetPrimaryUser returns the primary user (first user who interacted with the bot).
func (c *Client) GetPrimaryUser() id.UserID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.primaryUser
}

// RegisterEventHandlers registers external event handlers.
func (c *Client) RegisterEventHandlers(
	onRoomMessage func(roomID id.RoomID, sender id.UserID, content string),
	onDMMessage func(sender id.UserID, content string),
	onRoomJoin func(roomID id.RoomID, userID id.UserID),
	onRoomLeave func(roomID id.RoomID, userID id.UserID),
) {
	c.onRoomMessage = onRoomMessage
	c.onDMMessage = onDMMessage
	c.onRoomJoin = onRoomJoin
	c.onRoomLeave = onRoomLeave
}

// ServeHTTP handles HTTP requests for the appservice.
func (c *Client) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.as.Router.ServeHTTP(w, r)
}

// Stop stops the Matrix client.
func (c *Client) Stop() {
	c.cancel()
}

// GetBotUserID returns the bot user ID.
func (c *Client) GetBotUserID() id.UserID {
	return c.as.BotMXID()
}

// GetHomeserverDomain returns the homeserver domain.
func (c *Client) GetHomeserverDomain() string {
	return c.homeserver.Domain
}

// GetAppService returns the appservice instance.
func (c *Client) GetAppService() *appservice.AppService {
	return c.as
}
