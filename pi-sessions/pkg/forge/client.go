// pkg/forge - HTTP client for the forge REST API.
//
// The matrix appservice uses this package instead of the old
// pi-session-manager. forge is the single source of truth for sessions,
// messages, and tool calls; this client only does HTTP against its REST
// API.
//
// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package forge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a thin HTTP client for the forge API.
//
// All methods are safe for concurrent use.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// NewClient constructs a Client. baseURL is the forge root (e.g.
// "http://localhost:8080"); apiKey is sent on every request as
// "X-API-Key: <key>".
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http: &http.Client{
			// No global timeout: large prompts and slow tool calls
			// can sit on the wire for a while. Per-request timeouts
			// are set by the caller via context.
			Timeout: 0,
		},
	}
}

// do executes a request and decodes the JSON body into `out` if non-nil.
// Errors include the status code and the raw body so callers can show
// useful diagnostics.
func (c *Client) do(ctx context.Context, method, path string, body interface{}, out interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("forge: failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("forge: failed to build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("forge: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("forge: failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("forge: %s %s returned %d: %s", method, path, resp.StatusCode, string(respBody))
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("forge: failed to decode response: %w (body: %s)", err, string(respBody))
		}
	}

	return nil
}

// ============================================
// Types
// ============================================

// Profile mirrors the forge `profiles` row.
//
// We model it as a struct of pointers/omitempty so that the optional
// fields can be omitted from the create request.
type Profile struct {
	ID           string   `json:"id,omitempty"`
	Name         string   `json:"name"`
	Description  *string  `json:"description,omitempty"`
	Provider     string   `json:"provider"`
	Model        string   `json:"model"`
	BaseURL      *string  `json:"base_url,omitempty"`
	APIKey       *string  `json:"api_key,omitempty"`
	WorkingDir   string   `json:"working_dir"`
	GitURL       *string  `json:"git_url,omitempty"`
	GitRef       *string  `json:"git_ref,omitempty"`
	NixShell     *string  `json:"nix_shell,omitempty"`
	SystemPrompt *string  `json:"system_prompt,omitempty"`
	// Tools is decoded with a custom unmarshaller because
	// forge returns the column as a JSON string in responses
	// (Postgres jsonb columns get serialized through a
	// string-typed accessor on the way out). The appservice
	// sends Tools as a []string on the wire and never reads
	// the field, so the only direction that matters is the
	// response side.
	Tools ToolsField `json:"tools,omitempty"`
}

// ToolsField is forge's `tools` column, which the server
// returns as a JSON-encoded string (e.g. `"[\"bash\"]"`) and
// the client sends as a plain array. Custom unmarshalling
// accepts both forms so the appservice works with both old
// rows (string) and any future rows (array).
type ToolsField []string

func (t *ToolsField) UnmarshalJSON(data []byte) error {
	// Empty/null → no tools
	if len(data) == 0 || string(data) == "null" {
		*t = nil
		return nil
	}
	// Array form
	if data[0] == '[' {
		var arr []string
		if err := json.Unmarshal(data, &arr); err != nil {
			return err
		}
		*t = arr
		return nil
	}
	// String form (e.g. `"[\"bash\"]"`)
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*t = nil
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return err
	}
	*t = arr
	return nil
}

func (t ToolsField) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	return json.Marshal([]string(t))
}

// Session mirrors the forge `sessions` row.
type Session struct {
	ID         string  `json:"id"`
	ProfileID  string  `json:"profile_id"`
	Title      *string `json:"title,omitempty"`
	CellHost   *string `json:"cell_host,omitempty"`
	CellState  *any    `json:"cell_state,omitempty"`
	LastActive string  `json:"last_active"`
	CreatedAt  string  `json:"created_at"`
	EndedAt    *string `json:"ended_at,omitempty"`
	UserID     *string `json:"user_id,omitempty"`
}

// CreateSessionRequest is the body of POST /sessions.
type CreateSessionRequest struct {
	ProfileID string  `json:"profile_id"`
	Title     *string `json:"title,omitempty"`
}

// CreateSessionResponse is the body of POST /sessions (HTTP 201).
type CreateSessionResponse struct {
	Session     Session `json:"session"`
	WorkingDir  string  `json:"working_dir"`
}

// Message mirrors a row of the forge `messages` table.
//
// `sequence` is the per-session monotonic counter; we use it as the
// high-water mark for the event poller.
type Message struct {
	ID         string          `json:"id"`
	SessionID  string          `json:"session_id"`
	Sequence   int             `json:"sequence"`
	Role       string          `json:"role"` // "user" | "assistant" | "tool" | "system"
	Content    *string         `json:"content,omitempty"`
	ToolName   *string         `json:"tool_name,omitempty"`
	ToolInput  json.RawMessage `json:"tool_input,omitempty"`
	ToolCallID *string         `json:"tool_call_id,omitempty"`
	ToolOutput json.RawMessage `json:"tool_output,omitempty"`
	DurationMs *int64          `json:"duration_ms,omitempty"`
	CreatedAt  string          `json:"created_at"`
}

// CreateMessageRequest is the body of POST /messages.
type CreateMessageRequest struct {
	SessionID string `json:"session_id"`
	Content   string `json:"content"`
}

// SessionsList is the body of GET /sessions.
type SessionsList struct {
	Sessions []Session `json:"sessions"`
}

// ProfilesList is the body of GET /profiles.
type ProfilesList struct {
	Profiles []Profile `json:"profiles"`
}

// MessagesList is the body of GET /messages?session_id=...
type MessagesList struct {
	Messages []Message `json:"messages"`
}

// ============================================
// Profile endpoints
// ============================================

// ListProfiles returns all profiles (with optional pagination).
func (c *Client) ListProfiles(ctx context.Context, limit, offset int) ([]Profile, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	if offset > 0 {
		q.Set("offset", fmt.Sprintf("%d", offset))
	}
	path := "/profiles"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}

	var out ProfilesList
	if err := c.do(ctx, "GET", path, nil, &out); err != nil {
		return nil, err
	}
	return out.Profiles, nil
}

// FindProfileByWorkingDir returns the first profile whose working_dir
// matches exactly. Used to deduplicate profile creation per directory.
//
// Note: forge does not currently index profiles by working_dir, so this
// is a linear scan. With the small number of profiles the appservice
// creates (one per user per directory) this is fine.
func (c *Client) FindProfileByWorkingDir(ctx context.Context, workingDir string) (*Profile, error) {
	profiles, err := c.ListProfiles(ctx, 100, 0)
	if err != nil {
		return nil, err
	}
	for i := range profiles {
		if profiles[i].WorkingDir == workingDir {
			return &profiles[i], nil
		}
	}
	return nil, nil
}

// CreateProfile creates a new forge profile and returns the created row.
func (c *Client) CreateProfile(ctx context.Context, req Profile) (*Profile, error) {
	var out struct {
		Profile Profile `json:"profile"`
	}
	if err := c.do(ctx, "POST", "/profiles", req, &out); err != nil {
		return nil, err
	}
	return &out.Profile, nil
}

// GetProfile fetches a profile by id.
func (c *Client) GetProfile(ctx context.Context, id string) (*Profile, error) {
	var out struct {
		Profile Profile `json:"profile"`
	}
	if err := c.do(ctx, "GET", "/profiles/"+id, nil, &out); err != nil {
		return nil, err
	}
	return &out.Profile, nil
}

// ============================================
// Session endpoints
// ============================================

// CreateSession creates a new forge session against an existing profile
// and returns the session id + working dir.
func (c *Client) CreateSession(ctx context.Context, profileID string, title *string) (*CreateSessionResponse, error) {
	req := CreateSessionRequest{
		ProfileID: profileID,
		Title:     title,
	}
	var out CreateSessionResponse
	if err := c.do(ctx, "POST", "/sessions", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetSession fetches a session by id.
func (c *Client) GetSession(ctx context.Context, id string) (*Session, error) {
	var out Session
	if err := c.do(ctx, "GET", "/sessions/"+id, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListSessions returns all sessions known to forge.
func (c *Client) ListSessions(ctx context.Context) ([]Session, error) {
	var out SessionsList
	if err := c.do(ctx, "GET", "/sessions", nil, &out); err != nil {
		return nil, err
	}
	return out.Sessions, nil
}

// DeleteSession removes a session by id.
func (c *Client) DeleteSession(ctx context.Context, id string) error {
	return c.do(ctx, "DELETE", "/sessions/delete?id="+url.QueryEscape(id), nil, nil)
}

// ============================================
// Message endpoints
// ============================================

// SendMessage writes a user message to a session. The call returns
// immediately (forge's POST /messages is HTTP 202) and the agent's
// response is captured by the poller on subsequent calls to
// ListMessages.
func (c *Client) SendMessage(ctx context.Context, sessionID, content string) error {
	req := CreateMessageRequest{
		SessionID: sessionID,
		Content:   content,
	}
	return c.do(ctx, "POST", "/messages", req, nil)
}

// ListMessages returns all messages in a session, ordered by sequence.
//
// The poller calls this on a fixed interval and uses the `Sequence`
// field to detect new rows.
func (c *Client) ListMessages(ctx context.Context, sessionID string) ([]Message, error) {
	path := "/messages?session_id=" + url.QueryEscape(sessionID)
	var out MessagesList
	if err := c.do(ctx, "GET", path, nil, &out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// ============================================
// Health
// ============================================

// Health checks that the forge API is reachable. Used at startup.
func (c *Client) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.do(ctx, "GET", "/health", nil, nil)
}
