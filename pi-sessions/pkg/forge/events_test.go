// pkg/forge - Tests for the SSE event consumer.
//
// We stand up an httptest server that speaks SSE on a per-session
// path and verify that the consumer correctly translates forge
// events into SessionEvents.
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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// sseServer is a tiny stub of forge's SSE endpoint. For each
// `GET /sessions/{id}/events` request, it pops the next queued body
// and writes it back, then closes the connection. The consumer's
// reconnect logic is then responsible for picking up the next
// body.
type sseServer struct {
	*httptest.Server
	mu        sync.Mutex
	streams   map[string][]string // session_id -> queued SSE bodies (FIFO)
	connected atomic.Int32
}

func newSSEServer(t *testing.T) *sseServer {
	s := &sseServer{
		streams: make(map[string][]string),
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	return s
}

func (s *sseServer) serve(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/sessions/") || !strings.HasSuffix(r.URL.Path, "/events") {
		http.NotFound(w, r)
		return
	}
	sid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sessions/"), "/events")
	if r.Header.Get("Accept") != "text/event-stream" {
		http.Error(w, "expected text/event-stream", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	bodies, ok := s.streams[sid]
	if !ok || len(bodies) == 0 {
		s.mu.Unlock()
		// No data: close the connection so the consumer
		// doesn't hang waiting for more. The consumer's
		// reconnect-backoff or a Forget() call will
		// terminate the goroutine.
		w.WriteHeader(http.StatusOK)
		return
	}
	body := bodies[0]
	s.streams[sid] = bodies[1:]
	s.mu.Unlock()

	s.connected.Add(1)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "no flusher", http.StatusInternalServerError)
		return
	}
	if _, err := w.Write([]byte(body)); err != nil {
		return
	}
	flusher.Flush()
	// Returning from the handler closes the response body.
}

// enqueueBodies queues a sequence of bodies for a session id.
// Each connection drains one body. The consumer reconnects
// between bodies.
func (s *sseServer) enqueueBodies(sid string, bodies ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams[sid] = append(s.streams[sid], bodies...)
}

func (s *sseServer) connectionCount() int32 {
	return s.connected.Load()
}

// collect subscribes to the consumer's events and accumulates them.
type collect struct {
	mu   sync.Mutex
	evts []SessionEvent
}

func (c *collect) on(e SessionEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evts = append(c.evts, e)
}

func (c *collect) snapshot() []SessionEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]SessionEvent, len(c.evts))
	copy(out, c.evts)
	return out
}

func (c *collect) waitFor(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(c.snapshot()) >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return len(c.snapshot()) >= n
}

// fastConsumer returns a consumer configured for fast tests.
func fastConsumer(client *Client) *EventConsumer {
	return NewEventConsumer(EventConsumerConfig{
		Client:        client,
		Logger:        zerolog.Nop(),
		ReconnectMin:  20 * time.Millisecond,
		ReconnectMax:  50 * time.Millisecond,
		TypingQuiet:   50 * time.Millisecond,
		ReadTimeout:   5 * time.Second,
	})
}

// sseMessage formats a forge Message into an SSE event string.
func sseMessage(name string, payload any) string {
	data, _ := json.Marshal(payload)
	return fmt.Sprintf("event: %s\ndata: %s\n\n", name, string(data))
}

func TestConsumerEmitsMessageAndTypingEvents(t *testing.T) {
	ss := newSSEServer(t)
	client := NewClient(ss.URL, "")
	consumer := fastConsumer(client)
	col := &collect{}
	consumer.OnEvent(col.on)

	body := sseMessage("message", Message{
		ID: "m1", SessionID: "s-1", Sequence: 1, Role: "user", Content: strPtr("hi"),
	}) + sseMessage("message", Message{
		ID: "m2", SessionID: "s-1", Sequence: 2, Role: "assistant", Content: strPtr("hello"),
	})
	ss.enqueueBodies("s-1", body)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	if err := consumer.Track(ctx, "s-1"); err != nil {
		t.Fatalf("Track: %v", err)
	}
	defer consumer.Stop()

	if !col.waitFor(2, 2*time.Second) {
		t.Fatalf("expected at least 2 events, got %+v; connections=%d", col.snapshot(), ss.connectionCount())
	}
	evts := col.snapshot()
	var sawMessage, sawTyping bool
	for _, e := range evts {
		if e.Type == EventMessage && e.Content == "hello" {
			sawMessage = true
		}
		if e.Type == EventTypingStart {
			sawTyping = true
		}
	}
	if !sawMessage {
		t.Errorf("expected a message event with content 'hello', got %+v", evts)
	}
	if !sawTyping {
		t.Errorf("expected a typing_start event, got %+v", evts)
	}
}

func TestConsumerEmitsToolStartAndEnd(t *testing.T) {
	ss := newSSEServer(t)
	client := NewClient(ss.URL, "")
	consumer := fastConsumer(client)
	col := &collect{}
	consumer.OnEvent(col.on)

	tn1, tcid1 := "bash", "call-1"
	body := sseMessage("message", Message{
		ID: "m1", SessionID: "s-2", Sequence: 1, Role: "assistant",
		ToolName: &tn1, ToolCallID: &tcid1,
	}) + sseMessage("message", Message{
		ID: "m2", SessionID: "s-2", Sequence: 2, Role: "tool",
		ToolName: &tn1, ToolCallID: &tcid1,
		ToolOutput: json.RawMessage(`{"success":true}`),
	})
	ss.enqueueBodies("s-2", body)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	_ = consumer.Track(ctx, "s-2")
	defer consumer.Stop()

	if !col.waitFor(2, 2*time.Second) {
		t.Fatalf("expected at least 2 events, got %+v; connections=%d", col.snapshot(), ss.connectionCount())
	}
	var sawStart, sawEnd bool
	for _, e := range col.snapshot() {
		if e.Type == EventToolStart && e.ToolName == "bash" && e.ToolCallID == "call-1" {
			sawStart = true
		}
		if e.Type == EventToolEnd && e.ToolName == "bash" && e.ToolCallID == "call-1" && !e.IsError {
			sawEnd = true
		}
	}
	if !sawStart {
		t.Errorf("expected tool_start event")
	}
	if !sawEnd {
		t.Errorf("expected tool_end event with success=true")
	}
}

func TestConsumerFlagsToolErrors(t *testing.T) {
	ss := newSSEServer(t)
	client := NewClient(ss.URL, "")
	consumer := fastConsumer(client)
	col := &collect{}
	consumer.OnEvent(col.on)

	tn1, tcid1 := "read", "c-2"
	body := sseMessage("message", Message{
		ID: "m1", SessionID: "s-3", Sequence: 1, Role: "tool",
		ToolName: &tn1, ToolCallID: &tcid1,
		ToolOutput: json.RawMessage(`{"success":false,"error":"not found"}`),
	})
	ss.enqueueBodies("s-3", body)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	_ = consumer.Track(ctx, "s-3")
	defer consumer.Stop()

	if !col.waitFor(1, 2*time.Second) {
		t.Fatalf("expected at least 1 event, got %+v; connections=%d", col.snapshot(), ss.connectionCount())
	}
	var saw bool
	for _, e := range col.snapshot() {
		if e.Type == EventToolEnd && e.IsError {
			saw = true
		}
	}
	if !saw {
		t.Errorf("expected tool_end with is_error=true")
	}
}

func TestConsumerEmitsTypingStopOnTurnEnded(t *testing.T) {
	ss := newSSEServer(t)
	client := NewClient(ss.URL, "")
	consumer := fastConsumer(client)
	col := &collect{}
	consumer.OnEvent(col.on)

	body := sseMessage("message", Message{
		ID: "m1", SessionID: "s-4", Sequence: 1, Role: "user", Content: strPtr("hi"),
	}) + sseMessage("message", Message{
		ID: "m2", SessionID: "s-4", Sequence: 2, Role: "assistant", Content: strPtr("done"),
	}) + sseMessage("turn_ended", map[string]any{"session_id": "s-4"})
	ss.enqueueBodies("s-4", body)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	_ = consumer.Track(ctx, "s-4")
	defer consumer.Stop()

	if !col.waitFor(3, 2*time.Second) {
		t.Fatalf("expected at least 3 events, got %+v; connections=%d", col.snapshot(), ss.connectionCount())
	}
	var sawStop bool
	for _, e := range col.snapshot() {
		if e.Type == EventTypingStop {
			sawStop = true
		}
	}
	if !sawStop {
		t.Errorf("expected typing_stop after turn_ended, got %+v", col.snapshot())
	}
}

func TestConsumerEmitsTypingStopOnIdle(t *testing.T) {
	// No turn_ended event. The idle watcher should still
	// emit typing_stop after the typing-quiet window expires.
	ss := newSSEServer(t)
	client := NewClient(ss.URL, "")
	consumer := fastConsumer(client)
	col := &collect{}
	consumer.OnEvent(col.on)

	body := sseMessage("message", Message{
		ID: "m1", SessionID: "s-5", Sequence: 1, Role: "user", Content: strPtr("hi"),
	}) + sseMessage("message", Message{
		ID: "m2", SessionID: "s-5", Sequence: 2, Role: "assistant", Content: strPtr("done"),
	})
	ss.enqueueBodies("s-5", body)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	_ = consumer.Track(ctx, "s-5")
	defer consumer.Stop()

	// Wait for the user + assistant events, then a bit more
	// for the idle watcher to fire typing_stop.
	if !col.waitFor(2, 2*time.Second) {
		t.Fatalf("expected at least 2 events, got %+v; connections=%d", col.snapshot(), ss.connectionCount())
	}
	if !col.waitFor(3, 1*time.Second) {
		t.Fatalf("expected at least 3 events (incl typing_stop), got %+v", col.snapshot())
	}
	var sawStop bool
	for _, e := range col.snapshot() {
		if e.Type == EventTypingStop {
			sawStop = true
		}
	}
	if !sawStop {
		t.Errorf("expected typing_stop after idle window, got %+v", col.snapshot())
	}
}

func TestConsumerReconnectsForAdditionalEvents(t *testing.T) {
	// Two separate SSE deliveries, each with one assistant
	// message. The consumer should reconnect between them
	// and stitch the events together.
	ss := newSSEServer(t)
	client := NewClient(ss.URL, "")
	consumer := fastConsumer(client)
	col := &collect{}
	consumer.OnEvent(col.on)

	body1 := sseMessage("message", Message{
		ID: "m1", SessionID: "s-6", Sequence: 1, Role: "assistant", Content: strPtr("first"),
	})
	body2 := sseMessage("message", Message{
		ID: "m2", SessionID: "s-6", Sequence: 2, Role: "assistant", Content: strPtr("second"),
	})
	ss.enqueueBodies("s-6", body1, body2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	_ = consumer.Track(ctx, "s-6")
	defer consumer.Stop()

	if !col.waitFor(2, 3*time.Second) {
		t.Fatalf("expected 2 events across a reconnect, got %+v; connections=%d", col.snapshot(), ss.connectionCount())
	}
	var sawFirst, sawSecond bool
	for _, e := range col.snapshot() {
		if e.Type == EventMessage && e.Content == "first" {
			sawFirst = true
		}
		if e.Type == EventMessage && e.Content == "second" {
			sawSecond = true
		}
	}
	if !sawFirst || !sawSecond {
		t.Errorf("expected both first and second message events, got %+v", col.snapshot())
	}
	if ss.connectionCount() < 2 {
		t.Errorf("expected at least 2 SSE connections, got %d", ss.connectionCount())
	}
}

func TestConsumerForgetsSession(t *testing.T) {
	ss := newSSEServer(t)
	client := NewClient(ss.URL, "")
	consumer := fastConsumer(client)
	col := &collect{}
	consumer.OnEvent(col.on)

	body := sseMessage("message", Message{
		ID: "m1", SessionID: "s-7", Sequence: 1, Role: "user", Content: strPtr("hi"),
	})
	ss.enqueueBodies("s-7", body)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	_ = consumer.Track(ctx, "s-7")

	if !col.waitFor(1, 2*time.Second) {
		t.Fatalf("expected at least 1 event, got %+v; connections=%d", col.snapshot(), ss.connectionCount())
	}

	// Forget. The consumer should not pick up new rows.
	body2 := sseMessage("message", Message{
		ID: "m2", SessionID: "s-7", Sequence: 2, Role: "assistant", Content: strPtr("ignored"),
	})
	ss.enqueueBodies("s-7", body2)
	consumer.Forget("s-7")
	time.Sleep(200 * time.Millisecond)

	var msgCount int
	for _, e := range col.snapshot() {
		if e.Type == EventMessage {
			msgCount++
		}
	}
	if msgCount != 0 {
		t.Errorf("expected exactly 0 message events after Forget (user row emits typing_start, not message), got %d", msgCount)
	}
}

func TestReadSSEEventParsesMultilineData(t *testing.T) {
	body := "event: message\n" +
		"data: line one\n" +
		"data: line two\n" +
		"\n"
	r := bufio.NewReader(strings.NewReader(body))
	ev, err := readSSEEvent(r, time.Second)
	if err != nil {
		t.Fatalf("readSSEEvent: %v", err)
	}
	if ev.name != "message" {
		t.Errorf("name = %q, want message", ev.name)
	}
	if ev.data != "line one\nline two" {
		t.Errorf("data = %q, want %q", ev.data, "line one\nline two")
	}
}

func strPtr(s string) *string { return &s }
