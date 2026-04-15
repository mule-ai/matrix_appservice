# Implementation Plan - pi-matrix v2

## Overview

Refactor from extension-based architecture to server-based architecture using pi's RPC streaming mode.

## Changes from v1

### Old Architecture (Extension-based)
- Chrome extension connected to Go server via HTTP polling
- Session tied to browser tab's working directory
- No ability to create new sessions from Matrix
- HTTP long-polling for communication

### New Architecture (Server-based)
- Standalone Go server manages pi subprocesses via RPC
- User sends DM with directory path to create new session
- Server spawns `pi --mode rpc` subprocess in target directory
- Server creates dedicated Matrix room for each session
- Real-time bidirectional communication via RPC events
- Typing indicators tracked from pi events

## Implementation Phases

### Phase 1: Core Server Infrastructure ✓
- [x] Configuration loading (config.yaml)
- [x] Session struct with pi subprocess management
- [x] RPC communication with pi (stdin/stdout JSON)
- [x] Session manager for tracking multiple sessions
- [x] Basic Matrix client (mautrix-based)

### Phase 2: Matrix Integration ✓
- [x] Appservice registration and event handling
- [x] DM room creation and management
- [x] Session room creation per directory
- [x] Message routing (Matrix ↔ pi subprocess)
- [x] User invites to session rooms

### Phase 3: Session Lifecycle ✓
- [x] Start session: spawn pi subprocess via RPC
- [x] Session cleanup on exit
- [x] Session timeout handling (idle cleanup)
- [x] Graceful shutdown of pi subprocess

### Phase 4: Typing & Events ✓
- [x] Track agent activity (agent_start/end, tool calls)
- [x] Send typing indicators to Matrix
- [x] Forward streaming events from pi
- [x] Route messages from Matrix to pi

### Phase 5: DM Commands
- [x] `/start <path>` - Create new session
- [x] `/list` - List active sessions
- [x] `/stop [path]` - Stop a session
- [x] `/help` - Show help

### Phase 6: Polish & Testing
- [ ] Error handling improvements
- [ ] Logging improvements
- [ ] Unit tests
- [ ] Integration tests

## File Structure

```
pi-sessions/
├── cmd/pi-matrix/
│   └── main.go              # Entry point
├── pkg/
│   ├── config/
│   │   └── config.go       # Configuration
│   ├── matrix/
│   │   ├── client.go       # Matrix appservice client
│   │   ├── room.go         # Room representation
│   │   ├── room_registry.go # Room tracking
│   │   └── typing.go       # Typing state
│   ├── session/
│   │   ├── manager.go      # Session manager
│   │   └── session.go      # Session with pi subprocess
│   └── bridge/
│       └── bridge.go       # Main bridge logic
├── pi-matrix-bridge/       # TypeScript bridge for pi
│   ├── src/
│   │   └── index.ts        # Bridge implementation
│   └── package.json
├── config.yaml.example
├── Dockerfile
├── build.sh
├── docker-run.sh
├── go.mod
├── go.sum
├── SPEC.md
└── plan.md
```

## User Flow

### Creating a New Session
1. User sends DM to @pi-matrix:server with `/start /path/to/project`
2. Server validates directory exists
3. Server spawns `pi --mode rpc` subprocess in that directory
4. Server creates Matrix room "Pi Session: project"
5. Server invites user to session room
6. User joins room and starts chatting

### Interacting with Session
1. User sends message in session room
2. Server forwards to pi subprocess via RPC `prompt` command
3. pi streams events (agent_start, tool calls, etc.)
4. Server sends typing indicators
5. pi completes response
6. Server sends final message to Matrix room

### Session Continuation
1. Messages in same room continue same pi session
2. Session maintains conversation history
3. User can start new session with `/new` in same room (future)

## Testing Checklist

- [ ] Start server with config
- [ ] Register appservice with homeserver
- [ ] Send DM to bot
- [ ] Start session with `/start <path>`
- [ ] Verify room created
- [ ] Send message in room
- [ ] Receive response from pi
- [ ] Typing indicators appear
- [ ] Tool call notifications appear
- [ ] Session persists across messages
- [ ] Stop session with `/stop`
- [ ] Graceful shutdown

## Next Steps

1. Build and test the Go server
2. Register with Matrix homeserver
3. Test DM command handling
4. Test session room creation
5. Test message routing
6. Add streaming response handling
