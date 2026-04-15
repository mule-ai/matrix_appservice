// pi-matrix - Session representing a pi subprocess.
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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"go.mau.fi/pi-matrix/pkg/config"
)

// State represents the session state.
type State int

const (
	StatePending State = iota
	StateStarting
	StateRunning
	StateStopping
	StateStopped
)

func (s State) String() string {
	switch s {
	case StatePending: return "pending"
	case StateStarting: return "starting"
	case StateRunning: return "running"
	case StateStopping: return "stopping"
	case StateStopped: return "stopped"
	default: return "unknown"
	}
}

// Session represents a running pi subprocess.
type Session struct {
	ID        string
	Directory string
	UserID    string
	State     State

	PiConfig config.SessionManagerConfig

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	cmdLock sync.Mutex

	onEvent func(*Session, *SessionEvent)

	pendingRequests map[string]chan *RpcResponse
	requestMu       sync.RWMutex

	CreatedAt    time.Time
	LastActivity time.Time

	logger zerolog.Logger
	mu    sync.RWMutex
}

// newSession creates a new session.
func newSession(directory, userID string, cfg config.SessionManagerConfig, logger zerolog.Logger) *Session {
	absDir, _ := filepath.Abs(directory)
	if absDir == "" {
		absDir = directory
	}

	sessionID := uuid.New().String()
	return &Session{
		ID:        sessionID,
		Directory: absDir,
		UserID:    userID,
		State:     StatePending,
		PiConfig:  cfg,
		CreatedAt: time.Now(),
		LastActivity: time.Now(),
		logger: logger.With().Str("session_id", sessionID).Str("directory", absDir).Logger(),
		pendingRequests: make(map[string]chan *RpcResponse),
	}
}

// SetEventHandler sets the event handler.
func (s *Session) SetEventHandler(handler func(*Session, *SessionEvent)) {
	s.onEvent = handler
}

// SetState updates the session state.
func (s *Session) SetState(state State) {
	s.mu.Lock()
	s.State = state
	s.LastActivity = time.Now()
	s.mu.Unlock()

	s.logger.Info().Str("state", state.String()).Msg("session state changed")
}

// GetState returns the current state.
func (s *Session) GetState() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

// IsRunning returns true if the session is running.
func (s *Session) IsRunning() bool {
	return s.GetState() == StateRunning
}

// IsIdle returns true if the session has been idle.
func (s *Session) IsIdle(duration time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if duration == 0 {
		return false
	}
	return time.Since(s.LastActivity) > duration
}

// UpdateActivity updates the last activity time.
func (s *Session) UpdateActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastActivity = time.Now()
}

// Start starts the pi subprocess.
func (s *Session) Start(ctx context.Context) error {
	s.SetState(StateStarting)

	piPath, err := exec.LookPath(s.PiConfig.PiPath)
	if err != nil {
		if _, err := os.Stat(s.PiConfig.PiPath); err == nil {
			piPath = s.PiConfig.PiPath
		} else {
			s.SetState(StateStopped)
			return fmt.Errorf("pi not found: %s", s.PiConfig.PiPath)
		}
	}

	s.logger.Info().Str("pi_path", piPath).Str("directory", s.Directory).Msg("starting pi")

	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()

	// Clean up any existing process
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Kill()
		s.cmd.Wait()
	}

	// Use background context - we don't want the HTTP request context to kill the subprocess
	// Use --continue to resume session if this session was previously started
	args := []string{"--mode", "rpc"}
	if s.State != StatePending {
		// Session was previously started, resume it
		args = append(args, "--continue")
		s.logger.Info().Msg("resuming previous session")
	}
	s.cmd = exec.Command(piPath, args...)
	s.cmd.Dir = s.Directory

	env := os.Environ()
	// Add nvm node bin to PATH if using nvm-installed node
	nvmNodeBin := "/root/.nvm/versions/node/v20.18.1/bin"
	if _, err := os.Stat(nvmNodeBin); err == nil {
		env = append(env, "PATH="+nvmNodeBin+":"+os.Getenv("PATH"))
	}
	if s.PiConfig.AgentDir != "" {
		env = append(env, fmt.Sprintf("PI_AGENT_DIR=%s", s.PiConfig.AgentDir))
	}
	s.cmd.Env = env

	s.stdin, err = s.cmd.StdinPipe()
	if err != nil {
		s.SetState(StateStopped)
		return fmt.Errorf("failed to create stdin: %w", err)
	}

	s.stdout, err = s.cmd.StdoutPipe()
	if err != nil {
		s.stdin.Close()
		s.SetState(StateStopped)
		return fmt.Errorf("failed to create stdout: %w", err)
	}

	s.cmd.Stderr = os.Stderr

	if err := s.cmd.Start(); err != nil {
		s.stdin.Close()
		s.stdout.Close()
		s.SetState(StateStopped)
		return fmt.Errorf("failed to start pi: %w", err)
	}

	s.logger.Info().Msg("starting stdout reader goroutine")
	go s.readStdout()

	s.SetState(StateRunning)
	s.logger.Info().Str("pid", fmt.Sprintf("%d", s.cmd.Process.Pid)).Msg("pi started")

	return nil
}

// Stop stops the pi subprocess.
func (s *Session) Stop() error {
	s.SetState(StateStopping)

	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()

	if s.cmd == nil || s.cmd.Process == nil {
		s.SetState(StateStopped)
		return nil
	}

	// Send abort
	abortReq := RpcRequest{ID: uuid.New().String(), Type: "abort"}
	data, _ := json.Marshal(abortReq)
	fmt.Fprintf(s.stdin, "%s\n", string(data))

	// Wait briefly
	done := make(chan struct{})
	go func() {
		s.cmd.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.cmd.Process.Kill()
	}

	s.SetState(StateStopped)
	return nil
}

// SendPrompt sends a prompt to pi.
func (s *Session) SendPrompt(ctx context.Context, message string) error {
	if !s.IsRunning() {
		return fmt.Errorf("session not running")
	}

	req := RpcRequest{
		ID:      uuid.New().String(),
		Type:    "prompt",
		Message: message,
	}

	resp, err := s.sendRequest(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("prompt failed: %s", resp.Error)
	}

	return nil
}

// sendRequest sends an RPC request.
func (s *Session) sendRequest(ctx context.Context, req RpcRequest) (*RpcResponse, error) {
	respCh := make(chan *RpcResponse, 1)

	s.requestMu.Lock()
	s.pendingRequests[req.ID] = respCh
	s.requestMu.Unlock()

	s.cmdLock.Lock()
	data, _ := json.Marshal(req)
	_, err := fmt.Fprintf(s.stdin, "%s\n", string(data))
	s.cmdLock.Unlock()

	if err != nil {
		s.requestMu.Lock()
		delete(s.pendingRequests, req.ID)
		s.requestMu.Unlock()
		return nil, err
	}

	select {
	case resp := <-respCh:
		s.requestMu.Lock()
		delete(s.pendingRequests, req.ID)
		s.requestMu.Unlock()
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(60 * time.Second):
		s.requestMu.Lock()
		delete(s.pendingRequests, req.ID)
		s.requestMu.Unlock()
		return nil, fmt.Errorf("timeout")
	}
}

// readStdout reads from pi's stdout.
func (s *Session) readStdout() {
	s.logger.Debug().Msg("starting stdout reader")

	// Accumulator for incomplete lines
	var acc string
	buf := make([]byte, 8192)
	for {
		n, err := s.stdout.Read(buf)
		if err != nil {
			if acc != "" {
				s.handleLine(acc)
			}
			s.logger.Info().Err(err).Msg("stdout closed")
			s.SetState(StateStopped)
			return
		}

		if n > 0 {
			acc += string(buf[:n])
			
			// Process complete lines (ending with \n)
			for {
				if len(acc) == 0 {
					break
				}
				
				// Find next newline
				newlineIdx := -1
				for i := 0; i < len(acc); i++ {
					if acc[i] == '\n' {
						newlineIdx = i
						break
					}
				}
				
				if newlineIdx < 0 {
					// No newline found, wait for more data
					break
				}
				
				// Extract complete line
				line := acc[:newlineIdx]
				acc = acc[newlineIdx+1:]
				
				// Remove trailing \r if present
				if len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
				
				if line != "" {
					s.logger.Debug().Str("line", line).Msg("received from pi")
					s.handleLine(line)
				}
			}
		}
	}
}


// handleLine handles a single line of RPC output.
func (s *Session) handleLine(line string) {
	s.UpdateActivity()

	// Try response first
	var resp RpcResponse
	if err := json.Unmarshal([]byte(line), &resp); err == nil && resp.ID != "" {
		s.requestMu.RLock()
		ch, ok := s.pendingRequests[resp.ID]
		s.requestMu.RUnlock()
		if ok {
			ch <- &resp
			return
		}
	}

	// Try message_update event (has nested assistantMessageEvent)
	var msgUpdate MessageUpdateEvent
	if err := json.Unmarshal([]byte(line), &msgUpdate); err == nil && msgUpdate.Type == "message_update" {
		if msgUpdate.AssistantMessageEvent != nil { 
			// Nested struct unmarshal worked
			// Extract the nested event type and emit it
			// Use Delta for text_delta, Content for text_end
			text := msgUpdate.AssistantMessageEvent.Delta
			if text == "" {
				text = msgUpdate.AssistantMessageEvent.Content
			}
			nestedEvent := SessionEvent{
				Type:        msgUpdate.AssistantMessageEvent.Type,
				Text:        text,
				ContentIndex: msgUpdate.AssistantMessageEvent.ContentIndex,
			}
			s.logger.Debug().
				Str("event_type", nestedEvent.Type).
				Int("content_index", nestedEvent.ContentIndex).
				Str("text_preview", func() string { l := len(text); if l > 50 { return text[:50] + "..." }; return text }()).
				Msg("emitting nested event from message_update")
			if nestedEvent.Type != "" && s.onEvent != nil {
				go s.onEvent(s, &nestedEvent)
			}
		}
		return
	}
	// If we get here, check if it's a text_end directly (not nested in message_update)
	if strings.Contains(line, `"type":"text_end"`) {
		// Try to parse and show error
		var tmp map[string]interface{}
		if err := json.Unmarshal([]byte(line), &tmp); err != nil {
			s.logger.Debug().Err(err).Str("line_preview", func() string { l := len(line); if l > 100 { return line[:100] }; return line }()).Msg("text_end JSON parse error")
		}
		s.logger.Debug().Str("line_preview", func() string { l := len(line); if l > 100 { return line[:100] }; return line }()).Msg("found text_end in line but not parsed as message_update")
	}

	// Try tool_execution_start event
	var toolStart ToolExecutionStartEvent
	if err := json.Unmarshal([]byte(line), &toolStart); err == nil && toolStart.Type == "tool_execution_start" {
		event := SessionEvent{
			Type:     "tool_start",
			ToolName: toolStart.ToolName,
		}
		if s.onEvent != nil {
			go s.onEvent(s, &event)
		}
		return
	}

	// Try tool_execution_end event
	var toolEnd ToolExecutionEndEvent
	if err := json.Unmarshal([]byte(line), &toolEnd); err == nil && toolEnd.Type == "tool_execution_end" {
		event := SessionEvent{
			Type:     "tool_end",
			ToolName: toolEnd.ToolName,
			IsError:  toolEnd.IsError,
		}
		if s.onEvent != nil {
			go s.onEvent(s, &event)
		}
		return
	}

	// Try regular event
	var event SessionEvent
	if err := json.Unmarshal([]byte(line), &event); err == nil && event.Type != "" {
		if s.onEvent != nil {
			go s.onEvent(s, &event)
		}
	}
}

// RpcRequest represents an RPC request.
type RpcRequest struct {
	ID      string `json:"id,omitempty"`
	Type    string `json:"type"`
	Message string `json:"message,omitempty"`
}

// RpcResponse represents an RPC response.
type RpcResponse struct {
	ID      string `json:"id,omitempty"`
	Type    string `json:"type"`
	Command string `json:"command,omitempty"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// SessionEvent represents a pi session event.
type SessionEvent struct {
	Type        string          `json:"type"`
	Message     json.RawMessage `json:"message,omitempty"`
	ToolName    string          `json:"toolName,omitempty"`
	IsError     bool            `json:"isError,omitempty"`
	Text        string          `json:"text,omitempty"`
	ContentIndex int            `json:"contentIndex,omitempty"`
}

// MessageUpdateEvent represents a message_update event from pi (contains assistantMessageEvent)
type MessageUpdateEvent struct {
	Type                  string          `json:"type"`
	AssistantMessageEvent *AssistantEvent `json:"assistantMessageEvent,omitempty"`
}

// AssistantEvent represents the nested assistantMessageEvent in message_update
type AssistantEvent struct {
	Type         string `json:"type"`
	Delta        string `json:"delta,omitempty"`
	Content      string `json:"content,omitempty"`
	ContentIndex int    `json:"contentIndex,omitempty"`
}

// ToolExecutionStartEvent represents a tool_execution_start event from pi
type ToolExecutionStartEvent struct {
	Type      string `json:"type"`
	ToolCallID string `json:"toolCallId,omitempty"`
	ToolName  string `json:"toolName,omitempty"`
	Args      map[string]interface{} `json:"args,omitempty"`
}

// ToolExecutionEndEvent represents a tool_execution_end event from pi
type ToolExecutionEndEvent struct {
	Type      string `json:"type"`
	ToolCallID string `json:"toolCallId,omitempty"`
	ToolName  string `json:"toolName,omitempty"`
	Result    *struct {
		Content []map[string]interface{} `json:"content,omitempty"`
	} `json:"result,omitempty"`
	IsError  bool `json:"isError,omitempty"`
}
