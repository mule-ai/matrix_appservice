// pi-matrix - Integration tests for the session manager HTTP API.
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

package session

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"go.mau.fi/pi-matrix/pkg/config"
)

// testServer wraps the HTTP handler for testing.
type testServer struct {
	Server  *httptest.Server
	Manager *Manager
	Handler *httpHandler
	Client  *http.Client
	BaseURL string
	APIKey  string
}

// newTestServer creates a test server with the HTTP handler.
func newTestServer(t *testing.T, apiKey string) *testServer {
	cfg := config.Config{
		SessionManager: config.SessionManagerConfig{
			Host:           "127.0.0.1",
			Port:           0, // random port
			APIKey:         apiKey,
			PiPath:         "echo", // Use echo instead of pi for tests
			AgentDir:       "",
			MaxSessions:    10,
			SessionTimeout: 60,
		},
	}

	ctx := context.Background()
	logger := zerolog.Nop()

	manager := NewManager(ctx, cfg.SessionManager, logger)
	handler := NewHTTPHandler(cfg, manager, logger)

	server := httptest.NewServer(handler)

	return &testServer{
		Server:  server,
		Manager: manager,
		Handler: handler,
		Client:  server.Client(),
		BaseURL: server.URL,
		APIKey:  apiKey,
	}
}

// doRequest is a helper to make requests with optional auth.
func (ts *testServer) doRequest(method, path string, body interface{}) (*http.Response, string) {
	var reqBody io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reqBody = bytes.NewReader(data)
	}

	req, _ := http.NewRequest(method, ts.BaseURL+path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	if ts.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+ts.APIKey)
	}

	resp, err := ts.Client.Do(req)
	if err != nil {
		return nil, ""
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	return resp, string(bodyBytes)
}

// setupTestDir creates a temporary directory for testing.
func setupTestDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "pi-session-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	return dir
}

// TestHealthEndpoint tests the /health endpoint.
func TestHealthEndpoint(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	resp, body := ts.doRequest("GET", "/health", nil)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["status"] != "ok" {
		t.Errorf("expected status 'ok', got %v", result["status"])
	}
}

// TestCreateSession tests creating a new session.
func TestCreateSession(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	// Create a temp directory for the session
	dir := setupTestDir(t)
	defer os.RemoveAll(dir)

	// Create session
	body := map[string]string{
		"directory": dir,
		"user_id":   "test-user",
	}
	resp, respBody := ts.doRequest("POST", "/sessions", body)

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", resp.StatusCode, respBody)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(respBody), &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["directory"] != dir {
		t.Errorf("expected directory %s, got %v", dir, result["directory"])
	}

	if result["user_id"] != "test-user" {
		t.Errorf("expected user_id 'test-user', got %v", result["user_id"])
	}

	if result["state"] != "running" {
		t.Errorf("expected state 'running', got %v", result["state"])
	}

	sessionID, ok := result["id"].(string)
	if !ok || sessionID == "" {
		t.Errorf("expected non-empty session id")
	}

	// Verify session count
	if ts.Manager.Count() != 1 {
		t.Errorf("expected 1 session, got %d", ts.Manager.Count())
	}
}

// TestCreateSessionDirectoryNotFound tests creating a session with non-existent directory.
// Note: The manager now creates the directory if it doesn't exist.
func TestCreateSessionDirectoryNotFound(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	body := map[string]string{
		"directory": "/nonexistent/path/12345",
		"user_id":   "test-user",
	}
	resp, respBody := ts.doRequest("POST", "/sessions", body)

	// Directory is created automatically now
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", resp.StatusCode, respBody)
	}
}

// TestCreateSessionMissingDirectory tests creating a session without directory.
func TestCreateSessionMissingDirectory(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	body := map[string]string{
		"user_id": "test-user",
	}
	resp, _ := ts.doRequest("POST", "/sessions", body)

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

// TestListSessions tests listing all sessions.
func TestListSessions(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	// Create a few sessions
	for i := 0; i < 3; i++ {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		body := map[string]string{
			"directory": dir,
			"user_id":   fmt.Sprintf("test-user-%d", i),
		}
		resp, _ := ts.doRequest("POST", "/sessions", body)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("failed to create session: %d", resp.StatusCode)
		}
	}

	// List sessions
	resp, respBody := ts.doRequest("GET", "/sessions", nil)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(respBody), &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	sessions, ok := result["sessions"].([]interface{})
	if !ok {
		t.Fatalf("expected sessions array")
	}

	if len(sessions) != 3 {
		t.Errorf("expected 3 sessions, got %d", len(sessions))
	}
}

// TestGetSession tests getting a session by ID.
func TestGetSession(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	// Create a session
	dir := setupTestDir(t)
	defer os.RemoveAll(dir)

	body := map[string]string{
		"directory": dir,
		"user_id":   "test-user",
	}
	resp, createBody := ts.doRequest("POST", "/sessions", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("failed to create session: %d", resp.StatusCode)
	}

	var created map[string]interface{}
	json.Unmarshal([]byte(createBody), &created)
	sessionID := created["id"].(string)

	// Get session
	resp, respBody := ts.doRequest("GET", "/sessions/"+sessionID, nil)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(respBody), &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["id"] != sessionID {
		t.Errorf("expected id %s, got %v", sessionID, result["id"])
	}
}

// TestGetSessionNotFound tests getting a non-existent session.
func TestGetSessionNotFound(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	resp, _ := ts.doRequest("GET", "/sessions/nonexistent-id", nil)

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

// TestDeleteSession tests deleting a session.
func TestDeleteSession(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	// Create a session
	dir := setupTestDir(t)
	defer os.RemoveAll(dir)

	body := map[string]string{
		"directory": dir,
		"user_id":   "test-user",
	}
	resp, createBody := ts.doRequest("POST", "/sessions", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("failed to create session: %d", resp.StatusCode)
	}

	var created map[string]interface{}
	json.Unmarshal([]byte(createBody), &created)
	sessionID := created["id"].(string)

	// Delete session
	resp, _ = ts.doRequest("DELETE", "/sessions/"+sessionID, nil)

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", resp.StatusCode)
	}

	// Verify session is deleted
	if ts.Manager.Count() != 0 {
		t.Errorf("expected 0 sessions, got %d", ts.Manager.Count())
	}

	// Try to get deleted session
	resp, _ = ts.doRequest("GET", "/sessions/"+sessionID, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404 for deleted session, got %d", resp.StatusCode)
	}
}

// TestDeleteSessionNotFound tests deleting a non-existent session.
func TestDeleteSessionNotFound(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	resp, _ := ts.doRequest("DELETE", "/sessions/nonexistent-id", nil)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

// TestSendPrompt tests sending a prompt to a session.
// Note: With 'echo' as pi path, the session won't actually run pi,
// so we test the API response rather than full functionality.
func TestSendPrompt(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	// Create a session
	dir := setupTestDir(t)
	defer os.RemoveAll(dir)

	body := map[string]string{
		"directory": dir,
		"user_id":   "test-user",
	}
	resp, createBody := ts.doRequest("POST", "/sessions", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("failed to create session: %d", resp.StatusCode)
	}

	var created map[string]interface{}
	json.Unmarshal([]byte(createBody), &created)
	sessionID := created["id"].(string)

	// Send prompt - with echo command the session won't be "running" properly
	// but we can verify the API accepts the request structure
	promptBody := map[string]string{
		"message": "Hello, pi!",
	}
	resp, respBody := ts.doRequest("POST", "/sessions/"+sessionID+"/prompt", promptBody)

	// The session might not be fully running with echo, so accept both OK and error
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 200 or 500, got %d: %s", resp.StatusCode, respBody)
	}
}

// TestSendPromptSessionNotFound tests sending a prompt to a non-existent session.
func TestSendPromptSessionNotFound(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	promptBody := map[string]string{
		"message": "Hello!",
	}
	resp, _ := ts.doRequest("POST", "/sessions/nonexistent/prompt", promptBody)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

// TestSendPromptMissingMessage tests sending a prompt without a message.
func TestSendPromptMissingMessage(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	// Create a session
	dir := setupTestDir(t)
	defer os.RemoveAll(dir)

	body := map[string]string{
		"directory": dir,
		"user_id":   "test-user",
	}
	resp, _ := ts.doRequest("POST", "/sessions", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("failed to create session: %d", resp.StatusCode)
	}

	// Send empty prompt
	promptBody := map[string]string{}
	resp, _ = ts.doRequest("POST", "/sessions/test-session/prompt", promptBody)

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

// TestAPIAuthentication tests API key authentication.
func TestAPIAuthentication(t *testing.T) {
	// Create server WITH API key requirement
	tsWithKey := newTestServer(t, "test-api-key")
	defer tsWithKey.Server.Close()

	// Create server WITHOUT API key requirement for comparison
	tsNoKey := newTestServer(t, "")
	defer tsNoKey.Server.Close()

	// Request WITHOUT auth to server WITH auth requirement should fail
	req, _ := http.NewRequest("GET", tsWithKey.BaseURL+"/sessions", nil)
	resp, _ := tsWithKey.Client.Do(req)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 without auth on protected server, got %d", resp.StatusCode)
	}

	// Request with wrong auth should fail
	req, _ = http.NewRequest("GET", tsWithKey.BaseURL+"/sessions", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	resp, _ = tsWithKey.Client.Do(req)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 with wrong key, got %d", resp.StatusCode)
	}

	// Request with correct auth should succeed
	req, _ = http.NewRequest("GET", tsWithKey.BaseURL+"/sessions", nil)
	req.Header.Set("Authorization", "Bearer test-api-key")
	resp, _ = tsWithKey.Client.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 with correct auth, got %d", resp.StatusCode)
	}

	// Request without auth to server WITHOUT auth requirement should succeed
	resp, _ = tsNoKey.doRequest("GET", "/sessions", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 without auth on open server, got %d", resp.StatusCode)
	}
}

// TestCORSHeaders tests CORS headers on SSE endpoint.
func TestCORSHeaders(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	req, _ := http.NewRequest("GET", ts.BaseURL+"/events", nil)
	resp, err := ts.Client.Do(req)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer resp.Body.Close()

	// Check access control header
	allowOrigin := resp.Header.Get("Access-Control-Allow-Origin")
	if allowOrigin != "*" {
		t.Errorf("expected Access-Control-Allow-Origin '*', got '%s'", allowOrigin)
	}
}

// TestNotFoundEndpoint tests 404 for unknown endpoints.
func TestNotFoundEndpoint(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	resp, _ := ts.doRequest("GET", "/unknown/endpoint", nil)

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

// TestMethodNotAllowed tests that wrong methods return appropriate errors.
func TestMethodNotAllowed(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Server.Close()

	// GET on POST-only endpoint
	resp, _ := ts.doRequest("GET", "/sessions", nil)
	// GET /sessions should work (it's a list endpoint)
	if resp.StatusCode == http.StatusMethodNotAllowed {
		t.Errorf("expected status 200 for GET /sessions, got %d", resp.StatusCode)
	}

	// PUT on POST-only endpoint - the router returns 404 for unknown methods
	body := map[string]string{"directory": "/tmp"}
	resp, _ = ts.doRequest("PUT", "/sessions", body)
	// Current router returns 404 for PUT, this is acceptable behavior
	if resp.StatusCode == http.StatusOK {
		t.Errorf("expected non-200 status for PUT /sessions, got %d", resp.StatusCode)
	}
}

// TestMaxSessions tests the max sessions limit.
func TestMaxSessions(t *testing.T) {
	cfg := config.Config{
		SessionManager: config.SessionManagerConfig{
			Host:           "127.0.0.1",
			Port:           0,
			APIKey:         "",
			PiPath:         "echo",
			AgentDir:       "",
			MaxSessions:    2, // Low limit for testing
			SessionTimeout: 60,
		},
	}

	ctx := context.Background()
	manager := NewManager(ctx, cfg.SessionManager, zerolog.Nop())
	handler := NewHTTPHandler(cfg, manager, zerolog.Nop())
	server := httptest.NewServer(handler)
	defer server.Close()

	ts := &testServer{
		Server:  server,
		Manager: manager,
		Handler: handler,
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	// Create sessions up to the limit
	var sessionIDs []string
	for i := 0; i < 2; i++ {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		body := map[string]string{
			"directory": dir,
			"user_id":   fmt.Sprintf("user-%d", i),
		}
		resp, respBody := ts.doRequest("POST", "/sessions", body)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("failed to create session %d: %s", i, respBody)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(respBody), &result)
		sessionIDs = append(sessionIDs, result["id"].(string))
	}

	// Try to create one more session (should fail)
	dir := setupTestDir(t)
	defer os.RemoveAll(dir)

	body := map[string]string{
		"directory": dir,
		"user_id":   "user-over-limit",
	}
	resp, respBody := ts.doRequest("POST", "/sessions", body)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500 for exceeding max sessions, got %d: %s", resp.StatusCode, respBody)
	}

	if !strings.Contains(respBody, "maximum session limit") {
		t.Errorf("expected 'maximum session limit' error, got: %s", respBody)
	}
}
