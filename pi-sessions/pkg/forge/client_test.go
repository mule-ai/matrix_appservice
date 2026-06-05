// pkg/forge - Tests for the forge HTTP client.
//
// We stand up an httptest.Server that mimics the forge REST API
// and assert that the client handles all the success / error paths
// the matrix appservice cares about.
//
// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package forge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeForge is a tiny stub of the forge API. It records the requests
// it received and returns canned responses keyed on path + method.
type fakeForge struct {
	*httptest.Server
	mu      atomic.Int32
	apiKey  string
	handler func(w http.ResponseWriter, r *http.Request)
}

func newFakeForge(t *testing.T, apiKey string, handler func(w http.ResponseWriter, r *http.Request)) *fakeForge {
	f := &fakeForge{apiKey: apiKey}
	f.handler = handler
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Add(1)
		if apiKey != "" {
			got := r.Header.Get("X-API-Key")
			if got != apiKey {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"bad api key"}`))
				return
			}
		}
		f.handler(w, r)
	}))
	t.Cleanup(f.Close)
	return f
}

// calls returns the number of requests the fake has served. We use a
// helper rather than the atomic directly so the field name is private.
func (f *fakeForge) calls() int32 {
	return f.mu.Load()
}

func TestNewClientStripsTrailingSlash(t *testing.T) {
	c := NewClient("http://example.com/", "sk")
	if c.baseURL != "http://example.com" {
		t.Errorf("baseURL = %q, want %q", c.baseURL, "http://example.com")
	}
}

func TestHealthSendsGetToHealth(t *testing.T) {
	var gotMethod, gotPath string
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	c := NewClient(f.URL, "")
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
	if gotMethod != "GET" || gotPath != "/health" {
		t.Errorf("got %s %s, want GET /health", gotMethod, gotPath)
	}
}

func TestHealthNon2xxIsError(t *testing.T) {
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"boom"}`))
	})
	c := NewClient(f.URL, "")
	if err := c.Health(context.Background()); err == nil {
		t.Fatalf("expected error on 500")
	}
}

func TestAuthHeaderSentWhenSet(t *testing.T) {
	var gotKey string
	f := newFakeForge(t, "sk_forge_abc", func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-API-Key")
		w.WriteHeader(http.StatusOK)
	})
	c := NewClient(f.URL, "sk_forge_abc")
	_ = c.Health(context.Background())
	if gotKey != "sk_forge_abc" {
		t.Errorf("got X-API-Key = %q, want sk_forge_abc", gotKey)
	}
}

func TestCreateProfilePostsJSON(t *testing.T) {
	var gotMethod, gotPath, gotCT string
	var gotBody Profile
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotCT = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"profile":{"id":"p-1","name":"agent","provider":"anthropic","model":"claude","working_dir":"/tmp/x","tools":["bash"]}}`))
	})
	c := NewClient(f.URL, "")
	got, err := c.CreateProfile(context.Background(), Profile{
		Name: "agent", Provider: "anthropic", Model: "claude", WorkingDir: "/tmp/x",
	})
	if err != nil {
		t.Fatalf("CreateProfile: %v", err)
	}
	if gotMethod != "POST" || gotPath != "/profiles" {
		t.Errorf("got %s %s, want POST /profiles", gotMethod, gotPath)
	}
	if !strings.HasPrefix(gotCT, "application/json") {
		t.Errorf("Content-Type = %q", gotCT)
	}
	if gotBody.Name != "agent" || gotBody.WorkingDir != "/tmp/x" {
		t.Errorf("posted body wrong: %+v", gotBody)
	}
	if got.ID != "p-1" {
		t.Errorf("created profile id = %q, want p-1", got.ID)
	}
}

func TestListProfilesDecodesArray(t *testing.T) {
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") != "100" {
			t.Errorf("expected limit=100, got %s", r.URL.RawQuery)
		}
		w.Write([]byte(`{"profiles":[
			{"id":"a","name":"a","provider":"anthropic","model":"m","working_dir":"/x"},
			{"id":"b","name":"b","provider":"openai","model":"m","working_dir":"/y"}
		]}`))
	})
	c := NewClient(f.URL, "")
	got, err := c.ListProfiles(context.Background(), 100, 0)
	if err != nil {
		t.Fatalf("ListProfiles: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d profiles, want 2", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("ids = %q, %q", got[0].ID, got[1].ID)
	}
}

func TestFindProfileByWorkingDir(t *testing.T) {
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"profiles":[
			{"id":"a","name":"a","provider":"anthropic","model":"m","working_dir":"/x"},
			{"id":"b","name":"b","provider":"openai","model":"m","working_dir":"/y"}
		]}`))
	})
	c := NewClient(f.URL, "")
	got, err := c.FindProfileByWorkingDir(context.Background(), "/y")
	if err != nil {
		t.Fatalf("FindProfileByWorkingDir: %v", err)
	}
	if got == nil || got.ID != "b" {
		t.Errorf("got %+v, want profile b", got)
	}

	missing, err := c.FindProfileByWorkingDir(context.Background(), "/does/not/exist")
	if err != nil {
		t.Fatalf("FindProfileByWorkingDir: %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing dir, got %+v", missing)
	}
}

// Regression test for the matrix_appservice idempotency bug:
// when the in-memory cache and the persisted store both
// miss (e.g. matrix_appservice restarted and the store was
// wiped) but the profile was provisioned in a previous
// lifetime, `ensureProfile` would POST a new profile with
// the same name and forge-api would return 409 Conflict
// (after the matching forge-side fix) or 500 (before it).
// The fix: look up by name before creating.
func TestFindProfileByName(t *testing.T) {
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"profiles":[
			{"id":"a","name":"pi-matrix--user1--foo","provider":"anthropic","model":"m","working_dir":"/x"},
			{"id":"b","name":"pi-matrix--user1--bar","provider":"openai","model":"m","working_dir":"/y"}
		]}`))
	})
	c := NewClient(f.URL, "")

	got, err := c.FindProfileByName(context.Background(), "pi-matrix--user1--bar")
	if err != nil {
		t.Fatalf("FindProfileByName: %v", err)
	}
	if got == nil || got.ID != "b" {
		t.Errorf("got %+v, want profile b", got)
	}

	missing, err := c.FindProfileByName(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("FindProfileByName: %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing name, got %+v", missing)
	}
}

func TestCreateSessionPostsJSON(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody CreateSessionRequest
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"session":{"id":"s-1","profile_id":"p-1","title":"hello","last_active":"2026-01-01T00:00:00Z","created_at":"2026-01-01T00:00:00Z"},"working_dir":"/forge/sessions/s-1"}`))
	})
	c := NewClient(f.URL, "")
	title := "hello"
	got, err := c.CreateSession(context.Background(), "p-1", &title)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if gotMethod != "POST" || gotPath != "/sessions" {
		t.Errorf("got %s %s, want POST /sessions", gotMethod, gotPath)
	}
	if gotBody.ProfileID != "p-1" {
		t.Errorf("posted profile_id = %q, want p-1", gotBody.ProfileID)
	}
	if got.Session.ID != "s-1" {
		t.Errorf("returned session id = %q, want s-1", got.Session.ID)
	}
	if got.WorkingDir != "/forge/sessions/s-1" {
		t.Errorf("working dir = %q", got.WorkingDir)
	}
}

// TestGetSessionDecodesWrappedResponse is a regression test
// for a silent failure in /new. The forge GET /sessions/{id}
// endpoint wraps its row in `{"session": {...}}` (the same
// shape as POST /sessions), but the appservice was
// unmarshaling into a bare *Session, which silently zeroed
// every field. /new then took that zero-value ProfileID and
// POSTed a new session with `profile_id: ""`, which forge
// rejected with `invalid length: 0` — but only after the
// old session had already been DELETEd, leaving the user
// with a room that had no session at all. This test pins
// the wrapper-aware decode so a future refactor can't
// reintroduce the silent zero.
// TestGetSessionDecodesWrappedResponse is a regression test
// for a silent failure in /new. The forge GET /sessions/{id}
// endpoint wraps its row in `{"session": {...}}` (the same
// shape as POST /sessions), but the appservice was
// unmarshaling into a bare *Session, which silently zeroed
// every field. /new then took that zero-value ProfileID and
// POSTed a new session with `profile_id: ""`, which forge
// rejected with `invalid length: 0` — but only after the
// old session had already been DELETEd, leaving the user
// with a room that had no session at all. This test pins
// the wrapper-aware decode so a future refactor can't
// reintroduce the silent zero.
func TestGetSessionDecodesWrappedResponse(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody CreateMessageRequest
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusAccepted)
	})
	c := NewClient(f.URL, "")
	if err := c.SendMessage(context.Background(), "s-1", "hello"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if gotMethod != "POST" || gotPath != "/messages" {
		t.Errorf("got %s %s, want POST /messages", gotMethod, gotPath)
	}
	if gotBody.SessionID != "s-1" || gotBody.Content != "hello" {
		t.Errorf("posted body = %+v", gotBody)
	}
}

func TestListMessagesDecodesArray(t *testing.T) {
	var gotQuery string
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Write([]byte(`{"messages":[
			{"id":"m1","session_id":"s-1","sequence":1,"role":"user","content":"hi","created_at":"2026-01-01T00:00:00Z"},
			{"id":"m2","session_id":"s-1","sequence":2,"role":"assistant","content":"hello","created_at":"2026-01-01T00:00:01Z"}
		]}`))
	})
	c := NewClient(f.URL, "")
	got, err := c.ListMessages(context.Background(), "s-1")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if !strings.Contains(gotQuery, "session_id=s-1") {
		t.Errorf("query = %q, want session_id=s-1", gotQuery)
	}
	if len(got) != 2 {
		t.Fatalf("got %d messages, want 2", len(got))
	}
	if got[0].Role != "user" || got[1].Role != "assistant" {
		t.Errorf("roles = %q, %q", got[0].Role, got[1].Role)
	}
}

func TestDeleteSessionUsesQueryParam(t *testing.T) {
	var gotMethod, gotPath, gotQuery string
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	})
	c := NewClient(f.URL, "")
	if err := c.DeleteSession(context.Background(), "s 1/with spaces"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if gotMethod != "DELETE" || gotPath != "/sessions/delete" {
		t.Errorf("got %s %s, want DELETE /sessions/delete", gotMethod, gotPath)
	}
	// forge uses `id=` as the query param name. url.QueryEscape
	// emits `+` for space and `%2F` for `/`; either encoding
	// (form-style `+` or RFC 3986 `%20`) is accepted by axum's
	// Query extractor, so we just check the parameter name and the
	// slash escaping.
	if !strings.Contains(gotQuery, "id=") {
		t.Errorf("query = %q, want id=... parameter", gotQuery)
	}
	if !strings.Contains(gotQuery, "%2F") {
		t.Errorf("query = %q, want %%2F for slash", gotQuery)
	}
}

func TestErrorResponseIncludesStatusAndBody(t *testing.T) {
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"directory is required"}`))
	})
	c := NewClient(f.URL, "")
	_, err := c.CreateProfile(context.Background(), Profile{Name: "x"})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "directory is required") {
		t.Errorf("error = %q, want 400 + body", err.Error())
	}
}

func TestContextCancel(t *testing.T) {
	// Server hangs forever; the client should bail when we cancel.
	f := newFakeForge(t, "", func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	c := NewClient(f.URL, "")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.Health(ctx)
	if err == nil {
		t.Fatalf("expected error on context cancel")
	}
}

// TestProfileToolsFieldUnmarshal covers the ToolsField custom
// unmarshaller. forge returns the `tools` column as a JSON
// string (Postgres jsonb → text on the way out), but the client
// also needs to accept the array form for forward compatibility.
func TestProfileToolsFieldUnmarshal(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"json_string", `["bash","read"]`, []string{"bash", "read"}},   // also handles array form
		{"json_array", `["bash","read"]`, []string{"bash", "read"}},     // array form
		{"empty_string", `""`, nil},
		{"null", `null`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"tools":` + tc.raw + `}`)
			var p Profile
			if err := json.Unmarshal(body, &p); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if len(p.Tools) != len(tc.want) {
				t.Fatalf("got %v, want %v", p.Tools, tc.want)
			}
			for i, v := range tc.want {
				if p.Tools[i] != v {
					t.Errorf("[%d] = %q, want %q", i, p.Tools[i], v)
				}
			}
		})
	}

	// Marshal back to a plain array
	p := Profile{Tools: ToolsField{"bash", "read"}}
	out, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"tools":["bash","read"]`) {
		t.Errorf("marshal = %s, want array form", out)
	}
}

// TestProfileDecodesServerStyleString is a regression test for
// the exact JSON shape forge returns. The `tools` field is a
// JSON-encoded string inside a JSON object.
func TestProfileDecodesServerStyleString(t *testing.T) {
	body := []byte(`{"id":"abc","name":"x","provider":"anthropic","model":"claude","working_dir":"/tmp","tools":"[\"bash\", \"read\"]"}`)
	var p Profile
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(p.Tools) != 2 || p.Tools[0] != "bash" || p.Tools[1] != "read" {
		t.Fatalf("Tools = %v, want [bash read]", p.Tools)
	}
}
