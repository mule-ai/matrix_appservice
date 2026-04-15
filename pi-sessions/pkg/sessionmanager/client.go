// pi-matrix - Session manager client for the appservice.
// Communicates with the remote Pi Session Manager server via HTTP.
// Supports multiple session manager instances identified by machine name.
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
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// ManagerConfig contains configuration for a single session manager.
type ManagerConfig struct {
	Name   string `yaml:"name"`
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

// ClientConfig contains configuration for the session manager client.
type ClientConfig struct {
	Managers []ManagerConfig `yaml:"managers"`
	Logger   *zerolog.Logger
}

// SessionInfo represents information about a session.
type SessionInfo struct {
	ID          string `json:"id"`
	Directory   string `json:"directory"`
	UserID      string `json:"user_id"`
	MachineName string `json:"machine_name"`
	State       string `json:"state"`
}

// SessionEvent represents an event from a session.
type SessionEvent struct {
	Type        string `json:"type"`
	SessionID   string `json:"session_id"`
	MachineName string `json:"machine_name"`
	Content     string `json:"content,omitempty"`
	ToolName    string `json:"tool_name,omitempty"`
	IsError     bool   `json:"is_error,omitempty"`
}

// httpClient handles HTTP requests to a single session manager.
type httpClient struct {
	serverURL string
	apiKey    string
	logger    zerolog.Logger
	client    *http.Client
}

// newHTTPClient creates a new HTTP client for a session manager.
func newHTTPClient(url, apiKey string, logger zerolog.Logger) *httpClient {
	return &httpClient{
		serverURL: strings.TrimSuffix(url, "/"),
		apiKey:    apiKey,
		logger:    logger,
		client:    &http.Client{Timeout: 0},
	}
}

// doRequest performs an HTTP request with authentication.
func (c *httpClient) doRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var bodyReader *strings.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}
		bodyReader = strings.NewReader(string(data))
	} else {
		bodyReader = strings.NewReader("")
	}

	req, err := http.NewRequestWithContext(ctx, method, c.serverURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	return c.client.Do(req)
}

// Client is the appservice's client for communicating with multiple session managers.
type Client struct {
	managers map[string]*httpClient // machineName -> client
	logger   zerolog.Logger
	mu       sync.RWMutex

	// Event handlers
	onSessionEvent func(*SessionEvent)

	// SSE connections
	sseCtx    context.Context
	sseCancel context.CancelFunc
	sseWg     sync.WaitGroup
}

// NewClient creates a new session manager client.
func NewClient(cfg ClientConfig) (*Client, error) {
	logger := zerolog.Nop()
	if cfg.Logger != nil {
		logger = *cfg.Logger
	}

	c := &Client{
		managers: make(map[string]*httpClient),
		logger:   logger,
	}

	// Register all managers
	for _, m := range cfg.Managers {
		name := m.Name
		if name == "" {
			name = "default"
		}
		c.managers[name] = newHTTPClient(m.URL, m.APIKey, logger.With().Str("manager", name).Logger())
		logger.Info().Str("manager", name).Str("url", m.URL).Msg("registered session manager")
	}

	if len(c.managers) == 0 {
		return nil, fmt.Errorf("no session managers configured")
	}

	return c, nil
}

// Close closes all connections.
func (c *Client) Close() {
	if c.sseCancel != nil {
		c.sseCancel()
		c.sseWg.Wait()
	}
}

// RegisterEventHandler registers a handler for session events.
func (c *Client) OnSessionEvent(handler func(*SessionEvent)) {
	c.onSessionEvent = handler
}

// StartEventStream starts receiving events from all session managers via SSE.
// It automatically reconnects if connections are lost.
func (c *Client) StartEventStream(ctx context.Context) error {
	c.sseCtx, c.sseCancel = context.WithCancel(ctx)

	c.logger.Info().Int("count", len(c.managers)).Msg("starting event streams for all managers")

	// Start event stream for each manager
	for name, mgr := range c.managers {
		c.sseWg.Add(1)
		go func(name string, mgr *httpClient) {
			defer c.sseWg.Done()
			c.runEventStream(name, mgr)
		}(name, mgr)
	}

	return nil
}

// runEventStream handles connection and reconnection to a manager's event stream.
func (c *Client) runEventStream(machineName string, mgr *httpClient) {
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-c.sseCtx.Done():
			c.logger.Info().Str("manager", machineName).Msg("event stream context cancelled, stopping")
			return
		default:
		}

		c.logger.Info().Str("manager", machineName).Msg("connecting to event stream")

		resp, err := mgr.doRequest(c.sseCtx, "GET", "/events", nil)
		if err != nil {
			c.logger.Warn().Err(err).Str("manager", machineName).Msg("failed to connect to event stream")
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			c.logger.Warn().Int("status", resp.StatusCode).Str("manager", machineName).Msg("event stream returned error")
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Connected successfully, reset backoff
		backoff = time.Second
		c.logger.Info().Str("manager", machineName).Msg("connected to event stream")

		// Read events until connection closes
		c.readEvents(machineName, resp)

		// Connection closed, wait before reconnecting
		c.logger.Info().Str("manager", machineName).Msg("event stream disconnected, reconnecting...")
		time.Sleep(backoff)
	}
}

// readEvents reads SSE events from the response.
func (c *Client) readEvents(machineName string, resp *http.Response) {
	var dataBuf []byte
	buf := make([]byte, 4096)

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			for i := 0; i < n; i++ {
				if buf[i] == '\n' {
					line := strings.TrimSpace(string(dataBuf))
					dataBuf = dataBuf[:0]

					if line == "" {
						continue
					}

					if strings.HasPrefix(line, "data: ") {
						jsonData := strings.TrimPrefix(line, "data: ")
						c.handleEventData(machineName, jsonData)
					}
				} else {
					dataBuf = append(dataBuf, buf[i])
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
func (c *Client) handleEventData(machineName, jsonData string) {
	var event SessionEvent
	if err := json.Unmarshal([]byte(jsonData), &event); err != nil {
		c.logger.Warn().Err(err).Str("data", jsonData).Msg("failed to parse event")
		return
	}

	// Override machine name with the one we know from the connection
	event.MachineName = machineName

	c.logger.Debug().
		Str("session_id", event.SessionID).
		Str("machine_name", event.MachineName).
		Str("type", event.Type).
		Msg("received event")

	if c.onSessionEvent != nil {
		c.onSessionEvent(&event)
	}
}

// getManager returns the HTTP client for a machine, or nil if not found.
func (c *Client) getManager(machineName string) (*httpClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	mgr, ok := c.managers[machineName]
	if !ok {
		return nil, fmt.Errorf("unknown machine: %s", machineName)
	}
	return mgr, nil
}

// getDefaultManager returns the first registered manager.
func (c *Client) getDefaultManager() (*httpClient, string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.managers) == 0 {
		return nil, "", fmt.Errorf("no session managers configured")
	}

	for name, mgr := range c.managers {
		return mgr, name, nil
	}
	return nil, "", fmt.Errorf("no session managers configured")
}

// GetAvailableManagers returns a list of available machine names.
func (c *Client) GetAvailableManagers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.managers))
	for name := range c.managers {
		names = append(names, name)
	}
	return names
}

// CreateSession requests a new session from the specified manager.
func (c *Client) CreateSession(ctx context.Context, machineName, directory, userID string) (string, error) {
	var mgr *httpClient
	var err error

	if machineName == "" {
		mgr, machineName, err = c.getDefaultManager()
		if err != nil {
			return "", err
		}
	} else {
		mgr, err = c.getManager(machineName)
		if err != nil {
			return "", err
		}
	}

	reqBody := map[string]string{
		"directory":    directory,
		"user_id":      userID,
		"machine_name": machineName,
	}

	resp, err := mgr.doRequest(ctx, "POST", "/sessions", reqBody)
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
	// For delete, we don't know which manager has the session
	// Try each manager until one succeeds
	c.mu.RLock()
	managers := make(map[string]*httpClient)
	for k, v := range c.managers {
		managers[k] = v
	}
	c.mu.RUnlock()

	var lastErr error
	for name, mgr := range managers {
		err := c.tryDeleteSession(ctx, mgr, sessionID)
		if err == nil {
			return nil
		}
		lastErr = err
		c.logger.Debug().Err(err).Str("manager", name).Str("session_id", sessionID).Msg("delete failed on this manager")
	}

	return fmt.Errorf("session not found on any manager: %w", lastErr)
}

// tryDeleteSession attempts to delete a session from a specific manager.
func (c *Client) tryDeleteSession(ctx context.Context, mgr *httpClient, sessionID string) error {
	resp, err := mgr.doRequest(ctx, "DELETE", "/sessions/"+sessionID, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("session not found")
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	return nil
}

// SendPrompt sends a prompt to a session.
func (c *Client) SendPrompt(ctx context.Context, sessionID, message string) error {
	// For send prompt, we also need to try each manager
	c.mu.RLock()
	managers := make(map[string]*httpClient)
	for k, v := range c.managers {
		managers[k] = v
	}
	c.mu.RUnlock()

	var lastErr error
	for name, mgr := range managers {
		c.logger.Info().Str("manager", name).Str("session_id", sessionID).Msg("trying to send prompt")
		err := c.trySendPrompt(ctx, mgr, sessionID, message)
		if err == nil {
			c.logger.Info().Str("manager", name).Str("session_id", sessionID).Msg("prompt sent successfully")
			return nil
		}
		lastErr = err
		c.logger.Warn().Err(err).Str("manager", name).Str("session_id", sessionID).Msg("send prompt failed on this manager")
	}

	return fmt.Errorf("session not found on any manager: %w", lastErr)
}

// trySendPrompt attempts to send a prompt to a specific manager.
func (c *Client) trySendPrompt(ctx context.Context, mgr *httpClient, sessionID, message string) error {
	reqBody := map[string]string{
		"message": message,
	}

	resp, err := mgr.doRequest(ctx, "POST", "/sessions/"+sessionID+"/prompt", reqBody)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("session not found")
	}
	
	// Read response body for error message
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	
	if resp.StatusCode != http.StatusOK {
		// Check if agent is busy and we should steer
		if strings.Contains(bodyStr, "Agent is already processing") {
			c.logger.Info().Str("session_id", sessionID).Msg("agent busy, attempting to steer")
			
			// Retry with streamingBehavior: steer
			reqBodySteer := map[string]string{
				"message":           message,
				"streamingBehavior": "steer",
			}
			
			resp2, err2 := mgr.doRequest(ctx, "POST", "/sessions/"+sessionID+"/prompt", reqBodySteer)
			if err2 != nil {
				return fmt.Errorf("steer failed: %w", err2)
			}
			defer resp2.Body.Close()
			
			if resp2.StatusCode != http.StatusOK {
				body2, _ := io.ReadAll(resp2.Body)
				return fmt.Errorf("steer failed: %s", string(body2))
			}
			
			return nil
		}
		
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, bodyStr)
	}

	return nil
}

// ListSessions returns all active sessions from all managers.
func (c *Client) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	var allSessions []SessionInfo

	c.mu.RLock()
	managers := make(map[string]*httpClient)
	for k, v := range c.managers {
		managers[k] = v
	}
	c.mu.RUnlock()

	for name, mgr := range managers {
		sessions, err := c.listSessionsFromManager(ctx, mgr, name)
		if err != nil {
			c.logger.Warn().Err(err).Str("manager", name).Msg("failed to list sessions from manager")
			continue
		}
		allSessions = append(allSessions, sessions...)
	}

	return allSessions, nil
}

// listSessionsFromManager returns sessions from a specific manager.
func (c *Client) listSessionsFromManager(ctx context.Context, mgr *httpClient, machineName string) ([]SessionInfo, error) {
	resp, err := mgr.doRequest(ctx, "GET", "/sessions", nil)
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

	// Ensure machine name is set
	for i := range result.Sessions {
		if result.Sessions[i].MachineName == "" {
			result.Sessions[i].MachineName = machineName
		}
	}

	return result.Sessions, nil
}

// GetSession returns information about a specific session.
func (c *Client) GetSession(ctx context.Context, sessionID string) (*SessionInfo, error) {
	// Try each manager until we find the session
	c.mu.RLock()
	managers := make(map[string]*httpClient)
	for k, v := range c.managers {
		managers[k] = v
	}
	c.mu.RUnlock()

	for name, mgr := range managers {
		session, err := c.getSessionFromManager(ctx, mgr, sessionID, name)
		if err == nil && session != nil {
			return session, nil
		}
	}

	return nil, fmt.Errorf("session not found")
}

// getSessionFromManager returns a session from a specific manager.
func (c *Client) getSessionFromManager(ctx context.Context, mgr *httpClient, sessionID, machineName string) (*SessionInfo, error) {
	resp, err := mgr.doRequest(ctx, "GET", "/sessions/"+sessionID, nil)
	if err != nil {
		return nil, err
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

	if session.MachineName == "" {
		session.MachineName = machineName
	}

	return &session, nil
}

// GenerateRequestID generates a unique request ID.
func GenerateRequestID() string {
	return uuid.New().String()
}
