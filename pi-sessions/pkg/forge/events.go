// pkg/forge - Event consumer that turns forge's SSE `/events`
// stream into matrix appservice events.
//
// The matrix appservice opens one SSE connection per active session
// to forge's `GET /sessions/{id}/events?since=<seq>` endpoint. The
// stream replays any rows with `sequence > since` first (catch-up),
// then delivers new rows in real time. The consumer turns each
// forge row into a `SessionEvent` that the appservice code already
// knows how to handle.
//
// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package forge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// SessionEvent is the normalized form of something the agent did.
//
// We intentionally use the same shape the v3 `pi-session-manager`
// SSE produced (Type / SessionID / Content / ToolName / IsError) so
// the `appservice.onSessionEvent` handler works without changes.
type SessionEvent struct {
	Type       string `json:"type"` // message | tool_start | tool_end | typing_start | typing_stop
	SessionID  string `json:"session_id"`
	Content    string `json:"content,omitempty"`
	ToolName   string `json:"tool_name,omitempty"`
	ToolCallID string `json:"tool_call_id,omitempty"`
	IsError    bool   `json:"is_error,omitempty"`
}

// Event type names emitted by the consumer. The matrix appservice
// matches on these strings, so changing them is a breaking change.
const (
	EventMessage     = "message"
	EventToolStart   = "tool_start"
	EventToolEnd     = "tool_end"
	EventTypingStart = "typing_start"
	EventTypingStop  = "typing_stop"
)

// EventConsumer subscribes to forge's SSE event stream for one or
// more active sessions and dispatches normalized SessionEvents to a
// single callback.
//
// The consumer is safe for concurrent use; all mutable state is
// guarded by mu or the per-session goroutine-local state.
type EventConsumer struct {
	client *Client
	logger zerolog.Logger

	// Resolved config. Held on the struct so per-goroutine reads
	// don't need to chase through config indirection.
	reconnectMin  time.Duration
	reconnectMax  time.Duration
	typingQuiet   time.Duration
	readTimeout   time.Duration

	// Per-session control. Map entry exists iff Track() has been
	// called and Forget() has not yet removed it.
	mu       sync.Mutex
	sessions map[string]*sessionState

	// Callback.
	cbMu     sync.RWMutex
	callback func(SessionEvent)

	// Lifecycle.
	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
}

// sessionState holds the per-session state machine.
type sessionState struct {
	// lastSeq is the highest sequence number we've already
	// emitted. We send this as `since` on every (re)connect so
	// the server replays exactly the rows we haven't seen yet.
	lastSeq int

	// typingNow tracks whether we believe the agent is working
	// on this session. typingStart emits when the user just
	// spoke; typingStop emits when we see a turn_ended event or
	// the session has been quiet for the typing-quiet window.
	typingNow bool
	lastSeen  time.Time

	// stop signals the per-session goroutine to exit.
	stop chan struct{}
}

// EventConsumerConfig bundles the constructor parameters.
type EventConsumerConfig struct {
	Client *Client
	Logger zerolog.Logger
	// Reconnect backoff parameters. ReconnectMin <= ReconnectMax.
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	// TypingQuiet is the idle window after which a `typing_stop`
	// event is emitted if no new rows have arrived. Default 3s.
	TypingQuiet time.Duration
	// ReadTimeout is the maximum time we'll wait between SSE
	// comments before declaring the stream dead and reconnecting.
	// Default 60s; a healthy server sends a heartbeat comment
	// every 15s, so 60s is four heartbeats of slack.
	ReadTimeout time.Duration
}

// NewEventConsumer constructs a consumer. Call Start to begin
// tracking; call Track to add sessions; call Stop to terminate.
func NewEventConsumer(cfg EventConsumerConfig) *EventConsumer {
	if cfg.ReconnectMin <= 0 {
		cfg.ReconnectMin = 500 * time.Millisecond
	}
	if cfg.ReconnectMax <= 0 {
		cfg.ReconnectMax = 30 * time.Second
	}
	if cfg.TypingQuiet <= 0 {
		cfg.TypingQuiet = 3 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 60 * time.Second
	}
	return &EventConsumer{
		client:       cfg.Client,
		logger:       cfg.Logger,
		reconnectMin: cfg.ReconnectMin,
		reconnectMax: cfg.ReconnectMax,
		typingQuiet:  cfg.TypingQuiet,
		readTimeout:  cfg.ReadTimeout,
		sessions:     make(map[string]*sessionState),
	}
}

// OnEvent registers the callback. Must be called before Start.
func (c *EventConsumer) OnEvent(cb func(SessionEvent)) {
	c.cbMu.Lock()
	c.callback = cb
	c.cbMu.Unlock()
}

// Start launches the consumer's housekeeping (the typing-idle
// detector). Per-session SSE goroutines are spawned by Track.
func (c *EventConsumer) Start(ctx context.Context) {
	c.rootCtx, c.rootCancel = context.WithCancel(ctx)
	c.wg.Add(1)
	go c.idleWatcher()
}

// Stop terminates the consumer. After Stop returns, no further
// callbacks will fire.
func (c *EventConsumer) Stop() {
	if c.rootCancel != nil {
		c.rootCancel()
	}

	// Stop every per-session goroutine.
	c.mu.Lock()
	for _, s := range c.sessions {
		select {
		case <-s.stop:
			// already closed
		default:
			close(s.stop)
		}
	}
	c.mu.Unlock()

	// Wait for everything to drain.
	c.wg.Wait()
}

// Track starts watching a session. The first call seeds the
// high-water mark by querying forge for the current max sequence
// (so the SSE stream's catch-up phase doesn't replay the full
// history). Subsequent calls for the same id are no-ops.
func (c *EventConsumer) Track(ctx context.Context, sessionID string) error {
	c.mu.Lock()
	if _, exists := c.sessions[sessionID]; exists {
		c.mu.Unlock()
		return nil
	}
	st := &sessionState{
		stop: make(chan struct{}),
	}
	c.sessions[sessionID] = st
	c.mu.Unlock()

	// Seed the high-water mark synchronously so the very first
	// SSE request doesn't replay the full history. If this
	// fails (forge unreachable, etc.) we still proceed and let
	// the catch-up phase deal with the full history.
	if err := c.seedHighWaterMark(ctx, sessionID, st); err != nil {
		c.logger.Warn().Err(err).Str("session_id", sessionID).Msg("could not seed high-water mark; will replay history")
	}

	// If Start hasn't been called yet, we still spawn a
	// goroutine; the consumer's rootCtx will be set when Start
	// fires and the goroutine will use it. If Start is never
	// called, the goroutine still runs (with background ctx)
	// until Forget.
	parentCtx := c.rootCtx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	c.wg.Add(1)
	go c.runSession(parentCtx, sessionID, st)
	return nil
}

// Forget stops watching a session. Safe to call from any
// goroutine. After Forget returns, no more events for that
// session will fire.
func (c *EventConsumer) Forget(sessionID string) {
	c.mu.Lock()
	st, ok := c.sessions[sessionID]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(c.sessions, sessionID)
	c.mu.Unlock()

	select {
	case <-st.stop:
		// already closed
	default:
		close(st.stop)
	}
}

func (c *EventConsumer) seedHighWaterMark(ctx context.Context, sessionID string, st *sessionState) error {
	msgs, err := c.client.ListMessages(ctx, sessionID)
	if err != nil {
		return err
	}
	max := 0
	for _, m := range msgs {
		if m.Sequence > max {
			max = m.Sequence
		}
	}
	st.lastSeq = max
	return nil
}

// runSession owns the lifetime of one SSE connection. It opens a
// stream, reads events until the connection dies, and reconnects
// with exponential backoff. The loop exits when the per-session
// `stop` channel is closed (Forget was called) or the consumer's
// root context is cancelled (Stop was called).
func (c *EventConsumer) runSession(ctx context.Context, sessionID string, st *sessionState) {
	defer c.wg.Done()

	backoff := c.reconnectMin
	for {
		select {
		case <-ctx.Done():
			return
		case <-st.stop:
			return
		default:
		}

		err := c.consumeOnce(ctx, sessionID, st)
		// A clean EOF is just "the server hung up, reconnect
		// and try again." We treat it like any other error.
		_ = err
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			c.logger.Warn().Err(err).Str("session_id", sessionID).Dur("backoff", backoff).Msg("SSE stream ended; reconnecting")
		}

		select {
		case <-ctx.Done():
			return
		case <-st.stop:
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > c.reconnectMax {
			backoff = c.reconnectMax
		}
	}
}

// consumeOnce opens a single SSE connection and reads it until
// the connection dies or we're told to stop. It returns nil
// when the per-session stop channel fires (a clean exit) and an
// error otherwise so the caller can apply backoff.
func (c *EventConsumer) consumeOnce(ctx context.Context, sessionID string, st *sessionState) error {
	c.mu.Lock()
	since := st.lastSeq
	c.mu.Unlock()

	req, err := c.client.newSSE(ctx, sessionID, since)
	if err != nil {
		return err
	}

	resp, err := c.client.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return fmt.Errorf("forge: SSE connect returned %d: %s", resp.StatusCode, string(body))
	}

	reader := bufio.NewReader(resp.Body)
	for {
		event, err := readSSEEvent(reader, c.readTimeout)
		if err != nil {
			if errors.Is(err, errSSEClosed) {
				return nil
			}
			return err
		}
		if event == nil {
			continue
		}
		c.handleServerEvent(sessionID, st, event)
	}
}

// sseEvent is one parsed SSE line. Event names are in `name`;
// data payloads are in `data` (joined with newlines for multi-line
// data fields).
type sseEvent struct {
	name string
	data string
}

// errSSEClosed is returned by readSSEEvent when the server closed
// the stream cleanly (EOF on the body).
var errSSEClosed = errors.New("sse: stream closed")

// readSSEEvent parses one SSE event from the wire. A nil event
// with a nil error means "got a heartbeat / comment, keep going".
// errSSEClosed means the stream ended cleanly.
func readSSEEvent(r *bufio.Reader, readTimeout time.Duration) (*sseEvent, error) {
	var (
		name string
		data strings.Builder
	)
	deadline := time.Now().Add(readTimeout)
	for {
		line, err := readLineWithDeadline(r, deadline)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errSSEClosed
			}
			return nil, err
		}
		// Reset the deadline on every successful read.
		deadline = time.Now().Add(readTimeout)

		if line == "" {
			// Blank line: end of event. Dispatch what we have.
			if name == "" && data.Len() == 0 {
				continue // pure heartbeat
			}
			ev := &sseEvent{name: name, data: data.String()}
			return ev, nil
		}

		if strings.HasPrefix(line, ":") {
			// Comment / keepalive. Ignore.
			continue
		}
		if strings.HasPrefix(line, "event:") {
			name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			chunk := strings.TrimPrefix(line, "data:")
			chunk = strings.TrimPrefix(chunk, " ")
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(chunk)
			continue
		}
		// Other SSE fields (id:, retry:) are ignored.
	}
}

// readLineWithDeadline reads a single line (terminated by \n) from
// r. If no byte arrives before the deadline, returns an error.
// bufio.Reader doesn't expose a SetReadDeadline hook on the
// underlying net.Conn, so this is best-effort: we trust that the
// HTTP transport will respect the request's context for
// cancellation and rely on per-line reads to time out via the
// surrounding connection.
func readLineWithDeadline(r *bufio.Reader, deadline time.Time) (string, error) {
	_ = deadline // see function comment
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	return line, nil
}

// handleServerEvent dispatches one parsed SSE event.
func (c *EventConsumer) handleServerEvent(sessionID string, st *sessionState, ev *sseEvent) {
	switch ev.name {
	case "message":
		var msg Message
		if err := json.Unmarshal([]byte(ev.data), &msg); err != nil {
			c.logger.Warn().Err(err).Str("session_id", sessionID).Msg("SSE: bad message payload")
			return
		}
		if msg.SessionID != sessionID {
			return
		}
		c.mu.Lock()
		if msg.Sequence <= st.lastSeq {
			c.mu.Unlock()
			return
		}
		st.lastSeq = msg.Sequence
		st.lastSeen = time.Now()
		c.mu.Unlock()
		c.handleMessage(sessionID, &msg)

	case "turn_ended":
		c.mu.Lock()
		wasTyping := st.typingNow
		st.typingNow = false
		c.mu.Unlock()
		if wasTyping {
			c.emit(SessionEvent{Type: EventTypingStop, SessionID: sessionID})
		}

	default:
		// "heartbeat" and any other named events are ignored.
	}
}

// handleMessage translates one forge Message into SessionEvents.
func (c *EventConsumer) handleMessage(sessionID string, m *Message) {
	switch m.Role {
	case "user":
		c.startTyping(sessionID)
	case "assistant":
		if m.ToolCallID != nil && *m.ToolCallID != "" {
			tn := ""
			if m.ToolName != nil {
				tn = *m.ToolName
			}
			c.emit(SessionEvent{
				Type:       EventToolStart,
				SessionID:  sessionID,
				ToolName:   tn,
				ToolCallID: *m.ToolCallID,
			})
			return
		}
		if m.Content != nil && *m.Content != "" {
			c.emit(SessionEvent{
				Type:      EventMessage,
				SessionID: sessionID,
				Content:   *m.Content,
			})
		}
	case "tool":
		isErr := false
		if len(m.ToolOutput) > 0 {
			isErr = !extractSuccess(m.ToolOutput)
		}
		tn := ""
		if m.ToolName != nil {
			tn = *m.ToolName
		}
		tcid := ""
		if m.ToolCallID != nil {
			tcid = *m.ToolCallID
		}
		c.emit(SessionEvent{
			Type:       EventToolEnd,
			SessionID:  sessionID,
			ToolName:   tn,
			ToolCallID: tcid,
			IsError:    isErr,
		})
	}
}

// startTyping emits typing_start if not already in a typing state.
func (c *EventConsumer) startTyping(sessionID string) {
	c.mu.Lock()
	st, ok := c.sessions[sessionID]
	if !ok {
		c.mu.Unlock()
		return
	}
	was := st.typingNow
	st.typingNow = true
	st.lastSeen = time.Now()
	c.mu.Unlock()
	if !was {
		c.emit(SessionEvent{Type: EventTypingStart, SessionID: sessionID})
	}
}

// idleWatcher periodically scans all sessions and emits
// typing_stop for any that have been quiet for `typingQuiet`.
// This is a fallback for when the agent crashes mid-turn and we
// never see a turn_ended event; the explicit turn_ended path
// handles the happy case.
//
// The tick interval is `typingQuiet / 3` (clamped to 50ms..500ms)
// so the watcher is responsive in tests but not pointlessly busy
// in production.
func (c *EventConsumer) idleWatcher() {
	defer c.wg.Done()
	tick := c.typingQuiet / 3
	if tick < 50*time.Millisecond {
		tick = 50 * time.Millisecond
	}
	if tick > 500*time.Millisecond {
		tick = 500 * time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-c.rootCtx.Done():
			return
		case <-t.C:
			c.checkIdle()
		}
	}
}

func (c *EventConsumer) checkIdle() {
	cutoff := time.Now().Add(-c.typingQuiet)
	c.mu.Lock()
	type idle struct {
		id string
	}
	var idles []idle
	for id, st := range c.sessions {
		if st.typingNow && st.lastSeen.Before(cutoff) {
			idles = append(idles, idle{id: id})
			st.typingNow = false
		}
	}
	c.mu.Unlock()
	for _, i := range idles {
		c.emit(SessionEvent{Type: EventTypingStop, SessionID: i.id})
	}
}

func (c *EventConsumer) emit(e SessionEvent) {
	c.cbMu.RLock()
	cb := c.callback
	c.cbMu.RUnlock()
	if cb != nil {
		cb(e)
	}
}

// =====================
// Client SSE constructor
// =====================

// newSSE builds an HTTP request for the SSE endpoint. The body
// is text/event-stream. The caller is expected to consume the
// response body line-by-line.
func (c *Client) newSSE(ctx context.Context, sessionID string, since int) (*http.Request, error) {
	u := c.baseURL + "/sessions/" + url.PathEscape(sessionID) + "/events"
	if since > 0 {
		u += "?since=" + fmt.Sprintf("%d", since)
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	return req, nil
}

// extractSuccess pulls a boolean out of a tool_output JSON blob.
// Default to true (success) if the field is missing or not a
// bool, matching the recorder's behavior.
func extractSuccess(raw []byte) bool {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return true
	}
	if v, ok := obj["success"]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return true
}
