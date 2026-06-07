// pkg/appservice - Tests for the forge-backed flow.
//
// We stand up a fake forge (httptest) and a fake matrix client
// (implements the matrixClient interface) and exercise the
// appservice's command handlers end-to-end.
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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/pi-matrix/pkg/config"
	"go.mau.fi/pi-matrix/pkg/forge"
	"go.mau.fi/pi-matrix/pkg/matrix"
	"go.mau.fi/pi-matrix/pkg/store"
)

// ============================================
// fake matrix client
// ============================================

type fakeMx struct {
	mu      sync.Mutex
	bot     id.UserID
	msgs    map[id.RoomID][]sentMsg
	notices map[id.RoomID][]sentMsg
	typing  map[id.RoomID]bool
	rooms   map[string]id.RoomID // by name
	// invites records every InviteUser call. Used to assert
	// the re-invite-on-idempotent-path behavior in CreateAgent
	// tests.
	invites  map[id.RoomID][]id.UserID
	creators int
}

type sentMsg struct {
	Body string
	HTML string
}

func newFakeMx() *fakeMx {
	return &fakeMx{
		bot:     "@pi-matrix:test",
		msgs:    make(map[id.RoomID][]sentMsg),
		notices: make(map[id.RoomID][]sentMsg),
		typing:  make(map[id.RoomID]bool),
		rooms:   make(map[string]id.RoomID),
	}
}

func (m *fakeMx) GetBotUserID() id.UserID { return m.bot }

func (m *fakeMx) CreateDMRoom(ctx context.Context, userID id.UserID) (id.RoomID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creators++
	s := strings.ReplaceAll(string(userID), "@", "")
	s = strings.ReplaceAll(s, ":", "")
	roomID := id.RoomID(fmt.Sprintf("!dm-%s:bot", s))
	return roomID, nil
}

func (m *fakeMx) CreateSessionRoom(ctx context.Context, sessionDir string, userID id.UserID) (*matrix.Room, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creators++
	// Stable room id keyed on the session dir so a second
	// /start of the same dir maps to the same room. The
	// appservice only ever creates one room per session so
	// collisions in tests don't happen.
	roomID := id.RoomID(fmt.Sprintf("!room-%x:bot", hashDir(sessionDir)))
	return &matrix.Room{
		ID:         roomID,
		Name:       "Pi: " + sessionDir,
		SessionDir: sessionDir,
		CreatedAt:  time.Now(),
	}, nil
}

// CreateSessionRoomWithMachine is exercised by CreateAgent.
// Records the machine name and working dir so the test can
// assert on the final room name format. Also records the
// initial invite (matching what the real
// matrix.Client.CreateSessionRoomWithMachine does via
// bot.CreateRoom's Invite field).
func (m *fakeMx) CreateSessionRoomWithMachine(ctx context.Context, machineName, sessionDir string, userID id.UserID) (*matrix.Room, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creators++
	key := machineName + "|" + sessionDir
	roomID := id.RoomID(fmt.Sprintf("!room-%x:bot", hashDir(key)))
	name := "Pi: " + machineName
	if sessionDir != "" {
		name = "Pi: " + machineName + ": " + sessionDir
	}
	if m.invites == nil {
		m.invites = make(map[id.RoomID][]id.UserID)
	}
	m.invites[roomID] = append(m.invites[roomID], userID)
	return &matrix.Room{
		ID:         roomID,
		Name:       name,
		SessionDir: sessionDir,
		CreatedAt:  time.Now(),
	}, nil
}

// InviteUser records the invite so tests can assert on it.
func (m *fakeMx) InviteUser(ctx context.Context, roomID id.RoomID, userID id.UserID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.invites == nil {
		m.invites = make(map[id.RoomID][]id.UserID)
	}
	m.invites[roomID] = append(m.invites[roomID], userID)
	return nil
}

func (m *fakeMx) SendMessage(ctx context.Context, roomID id.RoomID, content string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs[roomID] = append(m.msgs[roomID], sentMsg{Body: content})
	return nil
}

func (m *fakeMx) SendNotice(ctx context.Context, roomID id.RoomID, content string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notices[roomID] = append(m.notices[roomID], sentMsg{Body: content})
	return nil
}

func (m *fakeMx) SetTyping(ctx context.Context, roomID id.RoomID, typing bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.typing[roomID] = typing
	return nil
}

func (m *fakeMx) RegisterEventHandlers(
	onRoomMessage func(roomID id.RoomID, sender id.UserID, content string),
	onDMMessage func(sender id.UserID, content string),
	onRoomJoin func(roomID id.RoomID, userID id.UserID),
	onRoomLeave func(roomID id.RoomID, userID id.UserID),
) {
	// No-op in tests; we drive the appservice by calling its
	// handlers directly.
}

func (m *fakeMx) snapshot() (msgs, notices map[id.RoomID][]sentMsg, typing map[id.RoomID]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs = make(map[id.RoomID][]sentMsg, len(m.msgs))
	for k, v := range m.msgs {
		msgs[k] = append([]sentMsg{}, v...)
	}
	notices = make(map[id.RoomID][]sentMsg, len(m.notices))
	for k, v := range m.notices {
		notices[k] = append([]sentMsg{}, v...)
	}
	typing = make(map[id.RoomID]bool, len(m.typing))
	for k, v := range m.typing {
		typing[k] = v
	}
	return
}

// hashDir produces a stable, short hash of a string for use in
// fake room IDs.
func hashDir(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ============================================
// fake forge
// ============================================

type fakeForge struct {
	*httptest.Server
	mu          sync.Mutex
	profiles    map[string]forge.Profile
	sessions    map[string]forge.Session
	messages    map[string][]forge.Message
	nextProfN   int
	nextSessN   int
	nextMsgN    map[string]int
	healthCalls int
}

func newFakeForge() *fakeForge {
	f := &fakeForge{
		profiles: make(map[string]forge.Profile),
		sessions: make(map[string]forge.Session),
		messages: make(map[string][]forge.Message),
		nextMsgN: make(map[string]int),
	}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

func (f *fakeForge) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == "GET" && r.URL.Path == "/health":
		f.mu.Lock()
		f.healthCalls++
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case r.Method == "POST" && r.URL.Path == "/profiles":
		var p forge.Profile
		json.NewDecoder(r.Body).Decode(&p)
		f.mu.Lock()
		f.nextProfN++
		p.ID = fmt.Sprintf("p-%d", f.nextProfN)
		f.profiles[p.ID] = p
		f.mu.Unlock()
		json.NewEncoder(w).Encode(struct {
			Profile forge.Profile `json:"profile"`
		}{Profile: p})
	case r.Method == "GET" && r.URL.Path == "/profiles":
		f.mu.Lock()
		out := make([]forge.Profile, 0, len(f.profiles))
		for _, p := range f.profiles {
			out = append(out, p)
		}
		f.mu.Unlock()
		json.NewEncoder(w).Encode(forge.ProfilesList{Profiles: out})
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/profiles/"):
		// GET /profiles/<id> — used by CreateAgent to
		// sanity-check the supplied profile_id. Real forge
		// returns {"profile": {...}}.
		pid := strings.TrimPrefix(r.URL.Path, "/profiles/")
		f.mu.Lock()
		p, ok := f.profiles[pid]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(struct {
			Profile forge.Profile `json:"profile"`
		}{Profile: p})
	case r.Method == "POST" && r.URL.Path == "/sessions":
		var req forge.CreateSessionRequest
		json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		f.nextSessN++
		sid := fmt.Sprintf("s-%d", f.nextSessN)
		title := ""
		if req.Title != nil {
			title = *req.Title
		}
		sess := forge.Session{
			ID:        sid,
			ProfileID: req.ProfileID,
			Title:     &title,
			CreatedAt: time.Now().Format(time.RFC3339),
		}
		f.sessions[sid] = sess
		f.mu.Unlock()
		json.NewEncoder(w).Encode(forge.CreateSessionResponse{
			Session:    sess,
			WorkingDir: "/forge/sessions/" + sid,
		})
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/sessions/") && strings.HasSuffix(r.URL.Path, "/events"):
		// SSE endpoint. We take a snapshot under the lock,
		// then release it before writing to the response
		// body. Rows added after the snapshot will be
		// visible on the consumer's next reconnect.
		sid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sessions/"), "/events")
		since := 0
		if s := r.URL.Query().Get("since"); s != "" {
			fmt.Sscanf(s, "%d", &since)
		}
		f.mu.Lock()
		rows := []forge.Message{}
		for _, m := range f.messages[sid] {
			if m.Sequence > since {
				rows = append(rows, m)
			}
		}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, m := range rows {
			data, _ := json.Marshal(m)
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
		}
		flusher.Flush()
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/sessions/") && r.URL.Path != "/sessions":
		sid := strings.TrimPrefix(r.URL.Path, "/sessions/")
		f.mu.Lock()
		sess, ok := f.sessions[sid]
		f.mu.Unlock()
		if ok {
			// Real forge wraps the row in `{"session": {...}}`
			// (same as POST /sessions). The fake has to match
			// or the appservice's GetSession unmarshals into
			// a zero-value Session (it used to silently do
			// that and the bug only manifested in /new).
			json.NewEncoder(w).Encode(struct {
				Session forge.Session `json:"session"`
			}{Session: sess})
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	case r.Method == "DELETE" && r.URL.Path == "/sessions/delete":
		sid := r.URL.Query().Get("id")
		f.mu.Lock()
		delete(f.sessions, sid)
		delete(f.messages, sid)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "POST" && r.URL.Path == "/messages":
		var req forge.CreateMessageRequest
		json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		f.nextMsgN[req.SessionID]++
		msg := forge.Message{
			ID:        fmt.Sprintf("m-%d", f.nextMsgN[req.SessionID]),
			SessionID: req.SessionID,
			Sequence:  f.nextMsgN[req.SessionID],
			Role:      "user",
			Content:   &req.Content,
			CreatedAt: time.Now().Format(time.RFC3339),
		}
		f.messages[req.SessionID] = append(f.messages[req.SessionID], msg)
		f.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	case r.Method == "GET" && r.URL.Path == "/messages":
		sid := r.URL.Query().Get("session_id")
		f.mu.Lock()
		msgs := f.messages[sid]
		f.mu.Unlock()
		json.NewEncoder(w).Encode(forge.MessagesList{Messages: msgs})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeForge) addAssistantMessage(sessionID, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextMsgN[sessionID]++
	contentCopy := content
	m := forge.Message{
		ID:        fmt.Sprintf("m-%d", f.nextMsgN[sessionID]),
		SessionID: sessionID,
		Sequence:  f.nextMsgN[sessionID],
		Role:      "assistant",
		Content:   &contentCopy,
		CreatedAt: time.Now().Format(time.RFC3339),
	}
	f.messages[sessionID] = append(f.messages[sessionID], m)
}

func (f *fakeForge) addToolCall(sessionID, toolName, callID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextMsgN[sessionID]++
	tn := toolName
	tcid := callID
	m := forge.Message{
		ID:         fmt.Sprintf("m-%d", f.nextMsgN[sessionID]),
		SessionID:  sessionID,
		Sequence:   f.nextMsgN[sessionID],
		Role:       "assistant",
		ToolName:   &tn,
		ToolCallID: &tcid,
		CreatedAt:  time.Now().Format(time.RFC3339),
	}
	f.messages[sessionID] = append(f.messages[sessionID], m)
}

func (f *fakeForge) profileCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.profiles)
}

func (f *fakeForge) sessionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sessions)
}

// createProfile seeds a profile directly into the fake's
// in-memory map. Used by tests that want to pre-populate
// state without going through the HTTP serve() path (e.g.
// the CreateAgent tests, which mirror what forge's
// `forge-agent-setup` does before calling CreateAgent).
func (f *fakeForge) createProfile(p forge.Profile) forge.Profile {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextProfN++
	p.ID = fmt.Sprintf("p-%d", f.nextProfN)
	f.profiles[p.ID] = p
	return p
}

// createSession seeds a session. Mirrors createProfile.
func (f *fakeForge) createSession(s forge.Session) forge.Session {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSessN++
	sid := fmt.Sprintf("s-%d", f.nextSessN)
	s.ID = sid
	f.sessions[sid] = s
	return s
}

// ============================================
// harness
// ============================================

type harness struct {
	t        *testing.T
	cfg      *config.Config
	ff       *fakeForge
	mx       *fakeMx
	consumer *forge.EventConsumer
	as       *AppService
	store    *store.Store
	stopPol  func()
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	ff := newFakeForge()
	mx := newFakeMx()

	st := newTestStore(t)

	cfg := &config.Config{
		Homeserver: config.HomeserverConfig{Domain: "test"},
		Appservice: config.AppserviceConfig{Localpart: "pi-matrix"},
		API:        config.APIConfig{Port: 0},
		Bridge:     config.BridgeConfig{RoomNamePrefix: "Pi"},
		Forge: config.ForgeConfig{
			URL:            ff.URL,
			APIKey:         "",
			ReconnectMinMs: 20,
			ReconnectMaxMs: 50,
			TypingQuietMs:  50,
			DefaultProfile: config.ForgeDefaultProfile{
				Provider:     "anthropic",
				Model:        "claude-sonnet-4-20250514",
				SystemPrompt: "test prompt",
				Tools:        []string{"bash", "read"},
			},
		},
	}
	cfg.Normalize()

	fc := forge.NewClient(cfg.Forge.URL, cfg.Forge.APIKey)
	consumer := forge.NewEventConsumer(forge.EventConsumerConfig{
		Client:       fc,
		Logger:       zerolog.Nop(),
		ReconnectMin: 20 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
		TypingQuiet:  50 * time.Millisecond,
	})

	as := &AppService{
		config:       *cfg,
		mxClient:     mx,
		forge:        fc,
		consumer:     consumer,
		store:        st,
		sessionRooms: make(map[string]id.RoomID),
		profileCache: make(map[profileKey]string),
		logger:       zerolog.Nop(),
	}
	consumer.OnEvent(as.onSessionEvent)

	return &harness{
		t:        t,
		cfg:      cfg,
		ff:       ff,
		mx:       mx,
		consumer: consumer,
		as:       as,
		store:    st,
		stopPol:  func() { consumer.Stop() },
	}
}

func (h *harness) startPoller() {
	h.consumer.Start(context.Background())
}

func (h *harness) close() {
	h.stopPol()
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	tmpDB, err := os.CreateTemp("", "appservice-test-*.db")
	if err != nil {
		t.Fatalf("temp db: %v", err)
	}
	path := tmpDB.Name()
	tmpDB.Close()
	os.Remove(path)

	st, err := store.NewStore(path, zerolog.Nop())
	if err != nil {
		// In-memory fallback if the file approach fails for any
		// reason. SQLite in-memory mode works with sql.Open +
		// "file::memory:?cache=shared" but our store's URL parser
		// doesn't expect that format; we just abort the test.
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close(); os.Remove(path) })
	return st
}

// waitFor polls until pred() returns true or the deadline elapses.
func waitFor(t *testing.T, timeout time.Duration, pred func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return pred()
}

// ============================================
// Tests
// ============================================

func TestStartCommandMintsProfileAndSession(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/proj-a")

	if h.ff.profileCount() != 1 {
		t.Errorf("expected 1 forge profile to be created, got %d", h.ff.profileCount())
	}
	if h.ff.sessionCount() != 1 {
		t.Errorf("expected 1 forge session, got %d", h.ff.sessionCount())
	}

	// We should have sent at least one notice to the DM room.
	_, notices, _ := h.mx.snapshot()
	var sawStart bool
	for _, n := range notices {
		for _, msg := range n {
			if strings.Contains(msg.Body, "Session started!") {
				sawStart = true
			}
		}
	}
	if !sawStart {
		t.Errorf("expected a 'Session started' notice somewhere, got %+v", notices)
	}
}

func TestStartCommandReusesProfileForSameDir(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/proj-a")
	h.as.onDMMessage("@alice:test", "/start /tmp/proj-a")

	if h.ff.profileCount() != 1 {
		t.Errorf("expected 1 forge profile (reused), got %d", h.ff.profileCount())
	}
	if h.ff.sessionCount() != 2 {
		t.Errorf("expected 2 forge sessions (one per /start), got %d", h.ff.sessionCount())
	}
}

func TestStartCommandDifferentDirMintsNewProfile(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/proj-a")
	h.as.onDMMessage("@alice:test", "/start /tmp/proj-b")

	if h.ff.profileCount() != 2 {
		t.Errorf("expected 2 forge profiles (one per dir), got %d", h.ff.profileCount())
	}
}

func TestStartCommandDifferentUserSameDirReusesProfile(t *testing.T) {
	// The forge profile binds a working dir to a model config.
	// When two matrix users `/start` the same directory, we
	// reuse the existing profile so they share the same model,
	// system prompt, and tools. Each `/start` still mints its
	// own session against the shared profile.
	h := newHarness(t)
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/shared")
	h.as.onDMMessage("@bob:test", "/start /tmp/shared")

	if h.ff.profileCount() != 1 {
		t.Errorf("expected 1 shared profile, got %d", h.ff.profileCount())
	}
	if h.ff.sessionCount() != 2 {
		t.Errorf("expected 2 sessions (one per user), got %d", h.ff.sessionCount())
	}
}

func TestRoomMessageForwardsToForge(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// Spin up a session.
	h.as.onDMMessage("@alice:test", "/start /tmp/proj-c")
	if h.ff.sessionCount() != 1 {
		t.Fatalf("expected 1 session, got %d", h.ff.sessionCount())
	}
	sid := mustFirstSessionID(t, h.ff)

	// Find the room the appservice opened for this session.
	h.as.mu.Lock()
	roomID := h.as.sessionRooms[sid]
	h.as.mu.Unlock()
	if roomID == "" {
		t.Fatalf("appservice did not register a room for session %s", sid)
	}

	// Send a room message.
	h.as.onRoomMessage(roomID, "@alice:test", "hello agent")

	// Verify forge got the message.
	h.ff.mu.Lock()
	defer h.ff.mu.Unlock()
	msgs := h.ff.messages[sid]
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message in forge, got %d", len(msgs))
	}
	if msgs[0].Role != "user" {
		t.Errorf("expected role=user, got %q", msgs[0].Role)
	}
	if msgs[0].Content == nil || *msgs[0].Content != "hello agent" {
		t.Errorf("content = %v, want 'hello agent'", msgs[0].Content)
	}
}

func TestForgeEventLandsInRoom(t *testing.T) {
	h := newHarness(t)
	h.startPoller()
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/proj-d")
	if h.ff.sessionCount() != 1 {
		t.Fatalf("expected 1 session, got %d", h.ff.sessionCount())
	}
	sid := mustFirstSessionID(t, h.ff)

	h.as.mu.Lock()
	roomID := h.as.sessionRooms[sid]
	h.as.mu.Unlock()
	if roomID == "" {
		t.Fatalf("no room for session %s", sid)
	}

	// Inject an assistant message and wait for it to land in the
	// matrix room.
	h.ff.addAssistantMessage(sid, "hi from forge")

	ok := waitFor(t, 2*time.Second, func() bool {
		msgs, _, _ := h.mx.snapshot()
		for _, m := range msgs[roomID] {
			if m.Body == "hi from forge" {
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("expected the assistant message to land in the room, did not")
	}
}

// TestStartEventsReTracksRestoredSessions is a regression test
// for a silent failure after appservice restart: the
// sessionRooms map is restored from the persisted portal store
// (so user messages can still be forwarded to forge), but the
// forge event consumer was previously never told to track the
// restored sessions, so forge's responses never reached the
// room. StartEvents must call Track on every restored session
// after starting the consumer.
func TestStartEventsReTracksRestoredSessions(t *testing.T) {
	ff := newFakeForge()
	mx := newFakeMx()
	st := newTestStore(t)

	cfg := &config.Config{
		Homeserver: config.HomeserverConfig{Domain: "test"},
		Appservice: config.AppserviceConfig{Localpart: "pi-matrix"},
		API:        config.APIConfig{Port: 0},
		Bridge:     config.BridgeConfig{RoomNamePrefix: "Pi"},
		Forge: config.ForgeConfig{
			URL:            ff.URL,
			ReconnectMinMs: 20,
			ReconnectMaxMs: 50,
			TypingQuietMs:  50,
			DefaultProfile: config.ForgeDefaultProfile{
				Provider: "anthropic", Model: "claude-sonnet-4-20250514",
				SystemPrompt: "test", Tools: []string{"bash"},
			},
		},
	}
	cfg.Normalize()

	fc := forge.NewClient(cfg.Forge.URL, "")
	consumer := forge.NewEventConsumer(forge.EventConsumerConfig{
		Client:       fc,
		Logger:       zerolog.Nop(),
		ReconnectMin: 20 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
		TypingQuiet:  50 * time.Millisecond,
	})

	const (
		sessionID = "restored-sess-1"
		roomID    = "!restored-room:test"
	)
	// Persist a portal in the store as if a prior appservice
	// run had created this session and bound it to the room.
	if err := st.SavePortal(&store.Portal{
		SessionID:   sessionID,
		RoomID:      id.RoomID(roomID),
		RoomName:    string(id.RoomID(roomID)),
		PrimaryUser: "@alice:test",
		CreatedAt:   time.Now().Unix(),
	}); err != nil {
		t.Fatalf("SavePortal: %v", err)
	}

	// Seed the fake forge with the session and one message so
	// the SSE replay on (re)connect emits it. sequence=1 lets
	// the consumer's seedHighWaterMark (which calls ListMessages
	// and picks the max) find a non-zero high-water mark, so
	// the first connect does NOT replay the same message over
	// and over. (We want the message to come through the SSE
	// stream as a live replay after Track runs.)
	title := "restored"
	ff.sessions[sessionID] = forge.Session{
		ID:        sessionID,
		ProfileID: "p-1",
		Title:     &title,
	}

	as := &AppService{
		config:       *cfg,
		mxClient:     mx,
		forge:        fc,
		consumer:     consumer,
		store:        st,
		sessionRooms: make(map[string]id.RoomID),
		profileCache: make(map[profileKey]string),
		logger:       zerolog.Nop(),
	}
	consumer.OnEvent(as.onSessionEvent)

	// NewAppService would normally call restoreSessionRooms
	// here. We inline it because the harness constructor
	// already creates an AppService directly.
	portals, err := st.GetAllPortals()
	if err != nil {
		t.Fatalf("GetAllPortals: %v", err)
	}
	for _, p := range portals {
		as.sessionRooms[p.SessionID] = p.RoomID
	}
	if got := len(as.sessionRooms); got != 1 {
		t.Fatalf("expected 1 restored portal, got %d", got)
	}

	stop := as.StartEvents(context.Background())
	defer stop()

	// Add the forge message AFTER the consumer has been
	// started and is already connected (lastSeq=0 because
	// the messages list was empty at seed time). The fake
	// forge's SSE handler returns whatever rows have
	// sequence > since, so a reconnect with lastSeq=0 picks
	// it up. (This matches the production case: forge
	// publishes a message, the consumer is already running,
	// and on its next reconnect the row comes through.)
	ff.addAssistantMessage(sessionID, "hi from forge after restart")

	ok := waitFor(t, 3*time.Second, func() bool {
		msgs, _, _ := mx.snapshot()
		for _, m := range msgs[id.RoomID(roomID)] {
			if m.Body == "hi from forge after restart" {
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("restored session was not tracked by consumer; forge response never reached the room")
	}
}

func TestToolCallEventsLandInRoom(t *testing.T) {
	h := newHarness(t)
	h.startPoller()
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/proj-e")
	sid := mustFirstSessionID(t, h.ff)

	h.as.mu.Lock()
	roomID := h.as.sessionRooms[sid]
	h.as.mu.Unlock()

	h.ff.addToolCall(sid, "bash", "call-1")

	ok := waitFor(t, 2*time.Second, func() bool {
		_, notices, _ := h.mx.snapshot()
		for _, n := range notices[roomID] {
			if strings.Contains(n.Body, "Running bash") {
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("expected a 'Running bash' notice in the room")
	}
}

func TestNewCommandResetsSession(t *testing.T) {
	h := newHarness(t)
	h.startPoller()
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/proj-f")
	firstID := mustFirstSessionID(t, h.ff)
	if firstID == "" {
		t.Fatalf("expected a session id")
	}
	h.as.mu.Lock()
	roomID := h.as.sessionRooms[firstID]
	h.as.mu.Unlock()

	// Reset.
	h.as.onRoomMessage(roomID, "@alice:test", "/new")

	// /new deletes the old session and mints a new one. The
	// fake forge's session map now contains only the new id.
	h.ff.mu.Lock()
	var newID string
	for sid := range h.ff.sessions {
		if sid != firstID {
			newID = sid
		}
	}
	h.ff.mu.Unlock()
	if newID == "" {
		t.Fatalf("expected a fresh session id, got none (still %q)", firstID)
	}
	if newID == firstID {
		t.Errorf("expected a different session id after /new, got %q == %q", newID, firstID)
	}

	h.as.mu.Lock()
	if h.as.sessionRooms[firstID] != "" {
		t.Errorf("old session should have been removed from sessionRooms")
	}
	if h.as.sessionRooms[newID] != roomID {
		t.Errorf("new session should map to the same room")
	}
}

// TestNewCommandPreservesOldSessionOnCreateFailure locks in the
// fail-soft ordering: /new mints the fresh forge session first
// and only then retires the old one. If CreateSession fails
// (e.g. forge rejects the new request, or the network is down),
// the old session must still be live in forge and the room's
// portal binding must still point at it. The previous order
// (delete-then-create) deleted the old session, then failed to
// create the new one, leaving the user with a room that had
// no session and no obvious way back.
func TestNewCommandPreservesOldSessionOnCreateFailure(t *testing.T) {
	ff := newFakeForge()
	mx := newFakeMx()
	st := newTestStore(t)

	cfg := &config.Config{
		Homeserver: config.HomeserverConfig{Domain: "test"},
		Appservice: config.AppserviceConfig{Localpart: "pi-matrix"},
		API:        config.APIConfig{Port: 0},
		Bridge:     config.BridgeConfig{RoomNamePrefix: "Pi"},
		Forge: config.ForgeConfig{
			URL:            ff.URL,
			ReconnectMinMs: 20,
			ReconnectMaxMs: 50,
			TypingQuietMs:  50,
			DefaultProfile: config.ForgeDefaultProfile{
				Provider: "anthropic", Model: "claude-sonnet-4-20250514",
				SystemPrompt: "test", Tools: []string{"bash"},
			},
		},
	}
	cfg.Normalize()

	fc := forge.NewClient(cfg.Forge.URL, "")
	consumer := forge.NewEventConsumer(forge.EventConsumerConfig{
		Client:       fc,
		Logger:       zerolog.Nop(),
		ReconnectMin: 20 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
		TypingQuiet:  50 * time.Millisecond,
	})
	as := &AppService{
		config:       *cfg,
		mxClient:     mx,
		forge:        fc,
		consumer:     consumer,
		store:        st,
		sessionRooms: make(map[string]id.RoomID),
		profileCache: make(map[profileKey]string),
		logger:       zerolog.Nop(),
	}
	consumer.OnEvent(as.onSessionEvent)

	// Bootstrap a session normally.
	as.onDMMessage("@alice:test", "/start /tmp/proj-f")
	firstID := mustFirstSessionID(t, ff)
	if firstID == "" {
		t.Fatalf("expected a session id")
	}
	as.mu.Lock()
	roomID := as.sessionRooms[firstID]
	as.mu.Unlock()

	// Now arm the fake forge to reject the next /sessions POST.
	// We swap the serve() handler so CreateSession returns 422.
	ff.Server.Close()
	ff.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/sessions" {
			w.WriteHeader(http.StatusUnprocessableEntity)
			w.Write([]byte(`{"error":"forced failure for test"}`))
			return
		}
		// Everything else (GET /sessions/{id} for the pre-new
		// lookup, etc.) uses the existing fake handler.
		ff.serve(w, r)
	}))

	// Run /new. The new session creation will fail. The old
	// session must remain bound to the room.
	as.onRoomMessage(roomID, "@alice:test", "/new")

	as.mu.Lock()
	if as.sessionRooms[firstID] != roomID {
		t.Errorf("old session %s was dropped from sessionRooms on /new failure; the user is now in an unbindable room", firstID)
	}
	as.mu.Unlock()
	ff.mu.Lock()
	if _, ok := ff.sessions[firstID]; !ok {
		t.Errorf("old session %s was DELETEd from forge despite CreateSession failing; the user is now in an unbindable room", firstID)
	}
	ff.mu.Unlock()
}

func TestStartCommandWithoutArgsShowsUsage(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start")
	_, notices, _ := h.mx.snapshot()
	var sawUsage bool
	for _, n := range notices {
		for _, msg := range n {
			if strings.Contains(msg.Body, "Usage:") {
				sawUsage = true
			}
		}
	}
	if !sawUsage {
		t.Errorf("expected usage notice, got %+v", notices)
	}
}

func TestSteerCommandSendsMessage(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/proj-g")
	sid := mustFirstSessionID(t, h.ff)
	h.as.mu.Lock()
	roomID := h.as.sessionRooms[sid]
	h.as.mu.Unlock()

	h.as.onRoomMessage(roomID, "@alice:test", "/steer please focus on the auth bug")

	h.ff.mu.Lock()
	msgs := h.ff.messages[sid]
	h.ff.mu.Unlock()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Content == nil || *msgs[0].Content != "please focus on the auth bug" {
		t.Errorf("steer content = %v, want 'please focus on the auth bug'", msgs[0].Content)
	}
}

func TestProfileNameIsStable(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	h.as.onDMMessage("@alice:test", "/start /tmp/some-proj")
	h.as.onDMMessage("@alice:test", "/start /tmp/some-proj")
	if h.ff.profileCount() != 1 {
		t.Fatalf("expected 1 profile, got %d", h.ff.profileCount())
	}
	h.ff.mu.Lock()
	var p forge.Profile
	for _, x := range h.ff.profiles {
		p = x
	}
	h.ff.mu.Unlock()
	if !strings.Contains(p.Name, "alice") || !strings.Contains(p.Name, "some-proj") {
		t.Errorf("expected profile name to embed user and dir, got %q", p.Name)
	}
}

// mustFirstSessionID returns the first session id in the fake
// forge. Tests use it after a /start command.
func mustFirstSessionID(t *testing.T, f *fakeForge) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for sid := range f.sessions {
		return sid
	}
	return ""
}

func TestExtractStartPath(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"/data/jbutler/git/project", "/data/jbutler/git/project"},
		{"~/work", "~/work"},
		{"./relative", "./relative"},
		// Old v1 syntax: leading machine name is dropped
		{"dev /data/jbutler/git/project", "/data/jbutler/git/project"},
		{"abot /tmp/foo", "/tmp/foo"},
		// First token is already a path: keep it
		{"/data/jbutler/git/project extra", "/data/jbutler/git/project"},
		// Whitespace
		{"   /data/jbutler/git/project   ", "/data/jbutler/git/project"},
		// Single non-path token: kept verbatim (resolves to CWD-relative)
		{"dev", "dev"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := extractStartPath(tc.in)
			if got != tc.want {
				t.Errorf("extractStartPath(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestLooksLikeGitURL(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		// URLs: detected
		{"https://github.com/foo/bar.git", true},
		{"http://internal/git/repo", true},
		{"git@github.com:foo/bar.git", true},
		{"git://github.com/foo/bar.git", true},
		{"ssh://git@github.com/foo/bar.git", true},
		{"file:///srv/repos/foo.git", true},
		// Anything ending in .git
		{"foo/bar.git", true},
		// Paths: not detected as URLs
		{"/data/jbutler/git/project", false},
		{"~/work", false},
		{"./relative", false},
		{"../foo", false},
		// `git` alone (not a path either, but a bare token)
		{"git", false},
		// Empty / whitespace
		{"", false},
		{"   ", false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := looksLikeGitURL(tc.in)
			if got != tc.want {
				t.Errorf("looksLikeGitURL(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
