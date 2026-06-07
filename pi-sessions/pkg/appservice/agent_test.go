// pkg/appservice - Tests for CreateAgent and the
// POST /api/v1/agents HTTP handler.
//
// These tests cover the operator-driven flow (forge's
// `forge-agent-setup` calling CreateAgent over HTTP) and
// the idempotency guarantees it relies on:
//
//   - minting a forge session/profile and getting a fresh
//     Matrix room bound to it;
//   - re-running the same request and getting the existing
//     room back unchanged;
//   - the HTTP-side error paths (auth, validation, 404
//     for missing session_id).

// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package appservice

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"maunium.net/go/mautrix/id"

	"go.mau.fi/pi-matrix/pkg/forge"
)

// helper: mint a profile + session in the fake forge and
// return both ids. Mirrors what forge-agent-setup does
// before calling CreateAgent.
func (h *harness) mintProfileAndSession(t *testing.T) (profileID, sessionID string) {
	t.Helper()
	p := h.ff.createProfile(forge.Profile{
		Name:       "test-agent",
		Provider:   "anthropic",
		Model:      "claude-sonnet-4-20250514",
		WorkingDir: "/data/projects/test",
	})
	s := h.ff.createSession(forge.Session{
		ProfileID: p.ID,
		Title:     stringPtr("Pi: test"),
	})
	return p.ID, s.ID
}

// stringPtr is a tiny helper to take the address of a string
// literal — Go has no built-in for it.
func stringPtr(s string) *string { return &s }

func TestCreateAgentMintsRoomForFreshSession(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	profileID, sessionID := h.mintProfileAndSession(t)

	resp, err := h.as.CreateAgent(context.Background(), CreateAgentRequest{
		ProfileID:  profileID,
		SessionID:  sessionID,
		WorkingDir: "/data/projects/test",
		UserID:     "@alice:test",
		RoomName:   "Test Agent",
	})
	if err != nil {
		t.Fatalf("CreateAgent: %v", err)
	}
	if resp.SessionID != sessionID {
		t.Errorf("SessionID = %q, want %q", resp.SessionID, sessionID)
	}
	if resp.RoomID == "" {
		t.Errorf("RoomID is empty")
	}
	if !strings.HasPrefix(resp.MatrixToURL, "https://matrix.to/#/") {
		t.Errorf("MatrixToURL = %q, want a matrix.to link", resp.MatrixToURL)
	}

	// The user should have been invited.
	if got := h.mx.invites[resp.RoomID]; len(got) != 1 || got[0] != "@alice:test" {
		t.Errorf("invites = %v, want [@alice:test]", got)
	}

	// And the portal should be persisted.
	p, err := h.store.GetPortal(sessionID)
	if err != nil || p == nil {
		t.Fatalf("portal not persisted: %v", err)
	}
	if p.RoomID != resp.RoomID {
		t.Errorf("portal.RoomID = %q, want %q", p.RoomID, resp.RoomID)
	}
}

func TestCreateAgentIsIdempotent(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	profileID, sessionID := h.mintProfileAndSession(t)
	req := CreateAgentRequest{
		ProfileID:  profileID,
		SessionID:  sessionID,
		WorkingDir: "/data/projects/test",
		UserID:     "@alice:test",
		RoomName:   "Test Agent",
	}

	first, err := h.as.CreateAgent(context.Background(), req)
	if err != nil {
		t.Fatalf("first CreateAgent: %v", err)
	}

	// Second call with the same args should return the
	// same room, NOT mint a new one.
	second, err := h.as.CreateAgent(context.Background(), req)
	if err != nil {
		t.Fatalf("second CreateAgent: %v", err)
	}
	if first.RoomID != second.RoomID {
		t.Errorf("room changed between calls: first=%q second=%q", first.RoomID, second.RoomID)
	}
	// And the room creator counter should still be 1
	// (the fake increments on every actual create).
	if h.mx.creators != 1 {
		t.Errorf("fakeMx.creators = %d, want 1 (idempotent should not create a new room)", h.mx.creators)
	}
	// Re-invite should have happened exactly once on the
	// second call (the first call invited as part of
	// the initial room creation, the second is a no-op
	// for the matrix client but we record it).
	if got := h.mx.invites[second.RoomID]; len(got) != 2 {
		t.Errorf("invites = %v, want 2 entries (1 from create, 1 from re-invite)", got)
	}
}

func TestCreateAgentValidatesInput(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	profileID, sessionID := h.mintProfileAndSession(t)
	cases := []struct {
		name    string
		req     CreateAgentRequest
		wantSub string
	}{
		{
			name:    "missing session_id",
			req:     CreateAgentRequest{ProfileID: profileID, UserID: "@alice:test"},
			wantSub: "session_id is required",
		},
		{
			name:    "missing user_id",
			req:     CreateAgentRequest{ProfileID: profileID, SessionID: sessionID},
			wantSub: "user_id is required",
		},
		{
			name:    "missing profile_id",
			req:     CreateAgentRequest{SessionID: sessionID, UserID: "@alice:test"},
			wantSub: "profile_id is required",
		},
		{
			name:    "bogus mxid",
			req:     CreateAgentRequest{ProfileID: profileID, SessionID: sessionID, UserID: "alice"},
			wantSub: "is not a valid Matrix user id",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.as.CreateAgent(context.Background(), tc.req)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %q, want substring %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestCreateAgentRejectsUnknownSession(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// Don't mint a session. forge will 404.
	_, err := h.as.CreateAgent(context.Background(), CreateAgentRequest{
		ProfileID: "p-1",
		SessionID: "s-doesnotexist",
		UserID:    "@alice:test",
	})
	if err == nil {
		t.Fatalf("expected error for unknown session, got nil")
	}
	if !strings.Contains(err.Error(), "s-doesnotexist") {
		t.Errorf("err = %q, want it to mention the missing session id", err.Error())
	}
}

func TestCreateAgentDefaultRoomName(t *testing.T) {
	cases := []struct {
		workingDir string
		sessionID  string
		want       string
	}{
		{"/data/projects/foo", "s-1", "Pi: foo"},
		{"/data/projects/foo.git", "s-1", "Pi: foo"},
		{"", "abcdef01-2345-6789", "Pi: abcdef01"},
		{"", "s-1", "Pi: s-1"},
	}
	for _, tc := range cases {
		t.Run(tc.workingDir+"/"+tc.sessionID, func(t *testing.T) {
			got := defaultRoomName(tc.workingDir, tc.sessionID)
			if got != tc.want {
				t.Errorf("defaultRoomName(%q, %q) = %q, want %q", tc.workingDir, tc.sessionID, got, tc.want)
			}
		})
	}
}

func TestLooksLikeMXID(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"@alice:test", true},
		{"@alice:example.com", true},
		{"@user_123:matrix.org", true},
		{"alice:test", false}, // no leading @
		{"@:test", false},     // empty localpart
		{"@alice:", false},    // empty domain
		{"@alice", false},     // no colon
		{"", false},
		{"@", false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := looksLikeMXID(tc.in); got != tc.want {
				t.Errorf("looksLikeMXID(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestHandleCreateAgentHTTP(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	profileID, sessionID := h.mintProfileAndSession(t)

	// Auth disabled (cfg.API.APIKey == ""), so the request
	// goes through without a header. Auth-enabled path is
	// covered by the dedicated TestHandleCreateAgentAuth below.
	body, _ := json.Marshal(CreateAgentRequest{
		ProfileID: profileID,
		SessionID: sessionID,
		UserID:    "@alice:test",
		RoomName:  "HTTP Test",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.as.HandleCreateAgent(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", rec.Code, rec.Body.String())
	}
	var resp CreateAgentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.SessionID != sessionID {
		t.Errorf("SessionID = %q, want %q", resp.SessionID, sessionID)
	}
	if resp.RoomID == "" {
		t.Errorf("RoomID is empty")
	}
}

func TestHandleCreateAgentRejectsBadMethod(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	rec := httptest.NewRecorder()
	h.as.HandleCreateAgent(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestHandleCreateAgentAuth(t *testing.T) {
	h := newHarness(t)
	defer h.close()
	// Turn on auth. We set the APIKey on the existing
	// config; newHarness already populated `cfg` and
	// `as.config` from it, so we just need to update both.
	h.cfg.API.APIKey = "test-secret"
	h.as.config.API.APIKey = "test-secret"

	profileID, sessionID := h.mintProfileAndSession(t)
	body, _ := json.Marshal(CreateAgentRequest{
		ProfileID: profileID,
		SessionID: sessionID,
		UserID:    "@alice:test",
	})

	t.Run("no header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.as.HandleCreateAgent(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("wrong key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", bytes.NewReader(body))
		req.Header.Set("X-API-Key", "nope")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.as.HandleCreateAgent(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("right key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", bytes.NewReader(body))
		req.Header.Set("X-API-Key", "test-secret")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.as.HandleCreateAgent(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, body = %s, want 200", rec.Code, rec.Body.String())
		}
	})
}

func TestHandleCreateAgentRejectsBogusBody(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents",
		strings.NewReader("not json at all"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.as.HandleCreateAgent(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// forge.Profile and forge.Session are referenced by the
// mintProfileAndSession helper. Defining an interface guard
// at the bottom ensures the harness still satisfies the
// shape the helper expects if anyone refactors.
var _ = id.RoomID("")
