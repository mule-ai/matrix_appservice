// pi-matrix - Session manager client for the appservice.
// Communicates with the remote Pi Session Manager server via HTTP.
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

package sessionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// ClientConfig contains configuration for the session manager client.
type ClientConfig struct {
	ServerURL string
	APIKey    string
	Logger    *zerolog.Logger
}

// SessionInfo represents information about a session.
type SessionInfo struct {
	ID       string `json:"id"`
	Directory string `json:"directory"`
	UserID   string `json:"user_id"`
	State    string `json:"state"`
}

// SessionEvent represents an event from a session.
type SessionEvent struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Content   string `json:"content,omitempty"`
	ToolName  string `json:"tool_name,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
}

// Client is the appservice's client for communicating with the session manager.
type Client struct {
	serverURL string
	apiKey    string
	logger    zerolog.Logger

	httpClient *http.Client

	// Event handlers
	onSessionEvent func(*SessionEvent)

	// SSE connection for receiving events
	eventsCtx    context.Context
	eventsCancel context.CancelFunc
	eventsWg     sync.WaitGroup
}

// NewClient creates a new session manager client.
func NewClient(cfg ClientConfig) (*Client, error) {
	logger := zerolog.Nop()
	if cfg.Logger != nil {
		logger = *cfg.Logger
	}

	// Normalize server URL
	serverURL := strings.TrimSuffix(cfg.ServerURL, "/")

	c := &Client{
		serverURL:  serverURL,
		apiKey:     cfg.APIKey,
		logger:     logger,
		httpClient: &http.Client{Timeout: 0},
	}

	return c, nil
}

// Close closes the client connection.
func (c *Client) Close() {
	if c.eventsCancel != nil {
		c.eventsCancel()
		c.eventsWg.Wait()
	}
}

// RegisterEventHandler registers a handler for session events.
func (c *Client) OnSessionEvent(handler func(*SessionEvent)) {
	c.onSessionEvent = handler
}

// StartEventStream starts receiving events from the session manager via SSE.
// It automatically reconnects if the connection is lost.
func (c *Client) StartEventStream(ctx context.Context) error {
	c.eventsCtx, c.eventsCancel = context.WithCancel(ctx)

	c.logger.Info().Str("server", c.serverURL).Msg("starting event stream with auto-reconnect")

	// Start the event stream connection
	go c.runEventStreamWithReconnect()

	return nil
}

// runEventStreamWithReconnect handles connection and reconnection to the event stream.
func (c *Client) runEventStreamWithReconnect() {
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-c.eventsCtx.Done():
			c.logger.Info().Msg("event stream context cancelled, stopping")
			return
		default:
		}

		c.logger.Info().Str("server", c.serverURL).Msg("connecting to event stream")

		req, err := http.NewRequestWithContext(c.eventsCtx, "GET", c.serverURL+"/events", nil)
		if err != nil {
			c.logger.Error().Err(err).Msg("failed to create event stream request")
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if c.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+c.apiKey)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			c.logger.Warn().Err(err).Msg("failed to connect to event stream")
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			c.logger.Warn().Int("status", resp.StatusCode).Msg("event stream returned error")
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Connected successfully, reset backoff
		backoff = time.Second
		c.logger.Info().Msg("connected to event stream")

		// Read events until connection closes
		c.readEvents(resp)

		// Connection closed, wait before reconnecting
		c.logger.Info().Msg("event stream disconnected, reconnecting...")
		time.Sleep(backoff)
	}
}

// readEvents reads SSE events from the response.
func (c *Client) readEvents(resp *http.Response) {
	// Simple line-based reading for SSE
	// Format: "data: {json}\n\n"
	var dataBuf string

	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			for i := 0; i < n; i++ {
				if buf[i] == '\n' {
					line := strings.TrimSpace(dataBuf)
					if strings.HasPrefix(line, "data: ") {
						jsonData := strings.TrimPrefix(line, "data: ")
						c.handleEventData(jsonData)
					}
					dataBuf = ""
				} else {
					dataBuf += string(buf[i])
				}
			}
		}
		if err != nil {
			c.logger.Info().Err(err).Msg("event stream closed")
			return
		}
	}
}

// handleEventData parses and dispatches an event.
func (c *Client) handleEventData(jsonData string) {
	var event SessionEvent
	if err := json.Unmarshal([]byte(jsonData), &event); err != nil {
		c.logger.Warn().Err(err).Str("data", jsonData).Msg("failed to parse event")
		return
	}

	c.logger.Debug().
		Str("session_id", event.SessionID).
		Str("type", event.Type).
		Msg("received event")

	if c.onSessionEvent != nil {
		c.onSessionEvent(&event)
	}
}

// CreateSession requests a new session from the manager.
func (c *Client) CreateSession(ctx context.Context, directory, userID string) (string, error) {
	reqBody := map[string]string{
		"directory": directory,
		"user_id":   userID,
	}

	reqData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.serverURL+"/sessions", strings.NewReader(string(reqData)))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	return result.ID, nil
}

// DeleteSession requests deletion of a session.
func (c *Client) DeleteSession(ctx context.Context, sessionID string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", c.serverURL+"/sessions/"+sessionID, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	return nil
}

// SendPrompt sends a prompt to a session.
func (c *Client) SendPrompt(ctx context.Context, sessionID, message string) error {
	reqBody := map[string]string{
		"message": message,
	}

	reqData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.serverURL+"/sessions/"+sessionID+"/prompt", strings.NewReader(string(reqData)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	return nil
}

// ListSessions returns all active sessions.
func (c *Client) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.serverURL+"/sessions", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	var result struct {
		Sessions []SessionInfo `json:"sessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return result.Sessions, nil
}

// GetSession returns information about a specific session.
func (c *Client) GetSession(ctx context.Context, sessionID string) (*SessionInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.serverURL+"/sessions/"+sessionID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	var session SessionInfo
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &session, nil
}

// GenerateRequestID generates a unique request ID.
func GenerateRequestID() string {
	return uuid.New().String()
}
