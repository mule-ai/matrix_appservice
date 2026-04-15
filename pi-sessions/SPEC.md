# Pi Sessions Matrix Appservice - Distributed Architecture Specification

## Version
2.0.0

## Date Created
2026-04-14

## Status
Draft

---

## Overview

The pi-matrix system uses a **distributed architecture** with two separate services:

1. **Appservice** (`pi-matrix`) - Matrix protocol handling, lives centrally
2. **Session Manager** (`pi-session-manager`) - Manages pi subprocesses, can be distributed

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    pi-matrix (Appservice)                                   │
│                                                                            │
│  - Lives centrally (single instance)                                        │
│  - Handles Matrix protocol (DMs, rooms, events)                          │
│  - Receives DMs: /start, /list, /stop, /help                              │
│  - Creates Matrix rooms for sessions                                       │
│  - Routes messages to/from Session Manager                                 │
│  - Receives events via SSE from Session Manager                           │
│                                                                            │
└────────────────────────────┬──────────────────────────────────────────────┘
                             │ HTTP + SSE
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 pi-session-manager (Session Manager)                          │
│                                                                            │
│  - Can be deployed multiple instances (horizontally scalable)               │
│  - Spawns `pi --mode rpc` subprocesses                                    │
│  - Manages session lifecycle                                               │
│  - Streams events to Appservice via SSE                                   │
│  - Handles message routing to pi                                            │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Session 1: /path/to/project-a  ──►  pi --mode rpc                 │   │
│  │  Session 2: /path/to/project-b  ──►  pi --mode rpc                 │   │
│  │  Session N: /path/to/project-n  ──►  pi --mode rpc                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## User Flow

### 1. User Creates Session via DM

```
User ──DM: "/start /home/user/projects/myapp"──► Appservice
                                              │
                                              ▼
                                    ┌──────────────────────┐
                                    │ Appservice validates │
                                    │ Creates session via  │
                                    │ Session Manager API   │
                                    └──────────────────────┘
                                              │
                                              ▼
                                    ┌──────────────────────┐
                                    │ Session Manager      │
                                    │ Spawns pi subprocess │
                                    └──────────────────────┘
                                              │
                                              ▼
                                    ┌──────────────────────┐
                                    │ Appservice creates   │
                                    │ Matrix room, invites │
                                    │ user                 │
                                    └──────────────────────┘
```

### 2. User Interacts with Session

```
User ──Room Message──► Appservice ──HTTP POST──► Session Manager
                                                    │
                                                    ▼
                                              ┌──────────────┐
                                              │ pi subprocess│
                                              │ (via stdin)  │
                                              └──────────────┘
                                                    │
                                                    ▼ (SSE events)
                                              ┌──────────────┐
                                              │ Appservice   │
                                              │ sends to room│
                                              └──────────────┘
```

---

## Services

### Appservice (`pi-matrix`)

**Responsibilities:**
- Matrix appservice protocol
- DM handling with commands
- Room creation and management
- Message routing to Session Manager
- Receiving events via SSE

**Commands (via DM):**
- `/start <path>` - Create new session
- `/list` - List active sessions
- `/stop` - Stop user's active session
- `/help` - Show help

**API Endpoints (consumed):**
- `POST /sessions` - Create session
- `DELETE /sessions/{id}` - Delete session
- `POST /sessions/{id}/prompt` - Send prompt
- `GET /sessions` - List sessions
- `GET /events` - SSE event stream

### Session Manager (`pi-session-manager`)

**Responsibilities:**
- Spawn and manage pi subprocesses
- Handle RPC communication with pi
- Stream events back to Appservice
- Session lifecycle (timeouts, cleanup)

**API Endpoints (provided):**
| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| GET | /sessions | List all sessions |
| POST | /sessions | Create new session |
| GET | /sessions/{id} | Get session info |
| DELETE | /sessions/{id} | Delete session |
| POST | /sessions/{id}/prompt | Send prompt to session |
| GET | /events | SSE event stream |

**SSE Events:**
```json
{"type": "typing_start", "session_id": "..."}
{"type": "typing_stop", "session_id": "..."}
{"type": "message", "session_id": "...", "content": "..."}
{"type": "tool_start", "session_id": "...", "tool_name": "bash"}
{"type": "tool_end", "session_id": "...", "tool_name": "bash", "is_error": false}
```

---

## Configuration

### Example Config

```yaml
homeserver:
    address: http://localhost:8008
    domain: localhost

appservice:
    id: pi-matrix
    localpart: pi-matrix
    url: http://localhost:29318
    as_token: "${APPSERVICE_AS_TOKEN}"
    hs_token: "${APPSERVICE_HS_TOKEN}"

session_manager:
    # Appservice connects to this URL
    url: http://localhost:8081
    api_key: ""

api:
    host: 0.0.0.0
    port: 8080

session_manager:
    host: 0.0.0.0
    port: 8081
    pi_path: pi
    agent_dir: ~/.pi/agent
    max_sessions: 10
    session_timeout: 3600
```

---

## Deployment Options

### Single Machine

Both services on same host:
```bash
# Terminal 1
./pi-session-manager -c config.yaml

# Terminal 2
./pi-matrix -c config.yaml
```

### Distributed

Session Manager on remote/scalable hosts:
```bash
# Server 1
./pi-session-manager -c config.yaml

# Server 2
./pi-session-manager -c config.yaml

# Appservice (connects to load balancer)
./pi-matrix -c config.yaml  # session_manager.url points to LB
```

---

## File Structure

```
pi-sessions/
├── cmd/
│   ├── pi-matrix/              # Appservice
│   │   └── main.go
│   └── pi-session-manager/     # Session Manager
│       └── main.go
├── pkg/
│   ├── appservice/            # Appservice logic
│   │   └── appservice.go
│   ├── config/
│   │   └── config.go
│   ├── matrix/
│   │   ├── client.go
│   │   ├── room.go
│   │   ├── room_registry.go
│   │   └── typing.go
│   ├── sessionmanager/
│   │   └── client.go           # Client for appservice
│   └── session/
│       ├── manager.go          # Session manager core
│       ├── session.go          # Session (pi subprocess)
│       └── http_handler.go    # HTTP API
├── config.yaml.example
├── SPEC.md
└── README.md
```

---

## Security Considerations

1. **API Key Authentication**: Session Manager supports optional API key
2. **Path Validation**: Appservice validates directory paths
3. **Process Isolation**: Each pi runs in separate subprocess
4. **Environment Variables**: Sensitive config via environment variables

---

## Session Lifecycle

Sessions are tracked persistently even when pi exits. This is because pi in RPC mode processes a single prompt and exits.

### Session States

| State | Description |
|-------|-------------|
| `pending` | Session created, not started |
| `starting` | pi subprocess starting |
| `running` | pi subprocess running and responsive |
| `stopping` | Session being stopped |
| `stopped` | pi subprocess exited (session still tracked) |

### Prompt Flow

1. **Session is running**: Send prompt directly to pi
2. **Session is stopped**: Restart pi, then send prompt
3. **Session timeout**: Session is cleaned up if idle

### Automatic Restart

When a prompt is sent to a stopped session:

```
SendPrompt -> session.state == stopped -> Start() -> pi restarts -> prompt sent
```

This allows sessions to be maintained across multiple conversations without keeping pi running continuously.

### Session Persistence

- Sessions are tracked in memory
- pi session files are persisted in the session directory
- Conversation history is maintained across pi restarts

---

## Future Enhancements

- [ ] Load balancing for Session Manager instances
- [ ] Session persistence across restarts
- [ ] Per-user session limits
- [ ] Rate limiting
- [ ] Metrics and monitoring
