# pi-matrix - Matrix Appservice for pi Sessions

## Version
2.0.0

## Status
Active

---

## Overview

**pi-matrix** is a distributed Matrix appservice that bridges [pi coding agent](https://github.com/mariozechner/pi-coding-agent) sessions to Matrix rooms. Users can create and interact with pi sessions directly from Matrix by sending DMs to the appservice bot.

## Architecture

The system uses a distributed architecture with two separate services:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    pi-matrix (Appservice)                                   │
│                                                                            │
│  - Lives centrally (single instance, runs on the matrix homeserver)        │
│  - Handles Matrix protocol (DMs, rooms, events)                           │
│  - Receives DMs: /start, /list, /stop, /help                              │
│  - Creates Matrix rooms for sessions                                       │
│  - Routes messages to/from Session Manager                                 │
│  - Receives events via SSE from Session Manager                            │
│                                                                            │
└────────────────────────────┬──────────────────────────────────────────────┘
                             │ HTTP + SSE
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 pi-session-manager (Session Manager)                         │
│                                                                            │
│  - Runs locally (on same host as user)                                     │
│  - Spawns `pi --mode rpc` subprocesses                                     │
│  - Manages session lifecycle                                               │
│  - Streams events to Appservice via SSE                                    │
│  - Handles message routing to pi                                           │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Session 1: /path/to/project-a  ──►  pi --mode rpc                 │   │
│  │  Session 2: /path/to/project-b  ──►  pi --mode rpc                 │   │
│  │  Session N: /path/to/project-n  ──►  pi --mode rpc                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why Two Services?

- **Session Manager** runs locally because it spawns `pi` subprocesses that need access to the user's files, git repositories, and environment.
- **Appservice** runs on the Matrix homeserver because it needs to communicate with the Matrix server directly with minimal latency.
- Communication between them uses HTTP with SSE for event streaming.

## Services

### Appservice (`pi-matrix`)

**Location**: Runs on the matrix homeserver

**Responsibilities**:
- Matrix appservice protocol
- DM handling with commands
- Room creation and management
- Message routing to Session Manager
- Receiving events via SSE

**Commands (via DM)**:
| Command | Description |
|---------|-------------|
| `/start <path>` | Create new session in the specified directory |
| `/list` | List active sessions |
| `/stop` | Stop user's active session |
| `/help` | Show help |

**API Endpoints (consumed by appservice)**:
- `POST /sessions` - Create session
- `DELETE /sessions/{id}` - Delete session
- `POST /sessions/{id}/prompt` - Send prompt
- `GET /sessions` - List sessions
- `GET /events` - SSE event stream

### Session Manager (`pi-session-manager`)

**Location**: Runs locally

**Responsibilities**:
- Spawn and manage pi subprocesses
- Handle RPC communication with pi
- Stream events back to Appservice
- Session lifecycle (timeouts, cleanup)

**API Endpoints (provided)**:
| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| GET | /sessions | List all sessions |
| POST | /sessions | Create new session |
| GET | /sessions/{id} | Get session info |
| DELETE | /sessions/{id} | Delete session |
| POST | /sessions/{id}/prompt | Send prompt to session |
| GET | /events | SSE event stream |

**SSE Events**:
```json
{"type": "typing_start", "session_id": "..."}
{"type": "typing_stop", "session_id": "..."}
{"type": "message", "session_id": "...", "content": "..."}
{"type": "tool_start", "session_id": "...", "tool_name": "bash"}
{"type": "tool_end", "session_id": "...", "tool_name": "bash", "is_error": false}
```

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

### Automatic Restart

When a prompt is sent to a stopped session:

```
SendPrompt → session.state == stopped → Start() → pi restarts → prompt sent
```

This allows sessions to be maintained across multiple conversations without keeping pi running continuously.

## Configuration

### Session Manager (`/etc/pi-session-manager.yaml`)

```yaml
session_manager:
  host: 0.0.0.0
  port: 19081
  api_key: ""
  pi_path: /root/.nvm/versions/node/v20.18.1/bin/pi
  agent_dir: ~/.pi/agent
  max_sessions: 10
  session_timeout: 3600
```

### Appservice (`/etc/pi-matrix/config.yaml`)

```yaml
homeserver:
  address: http://localhost:8008
  domain: matrix.butler.ooo

appservice:
  id: pi-matrix
  localpart: pi-matrix
  url: http://0.0.0.0:8080
  as_token: "${APPSERVICE_AS_TOKEN}"
  hs_token: "${APPSERVICE_HS_TOKEN}"

session_manager:
  url: http://10.10.199.96:19081
  api_key: ""
```

## Deployment

### Systemd Services

**Session Manager** (local):
```bash
systemctl start pi-session-manager
systemctl stop pi-session-manager
systemctl restart pi-session-manager
```

**Appservice** (on matrix homeserver):
```bash
systemctl start pi-matrix
systemctl stop pi-matrix
systemctl restart pi-matrix
```

### Log Viewing

```bash
# Session manager logs
journalctl -u pi-session-manager -f

# Appservice logs
journalctl -u pi-matrix -f
```

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
│   ├── bridge/                # Bridge logic
│   ├── config/                # Configuration
│   ├── matrix/                # Matrix client (rooms, typing, etc.)
│   ├── session/               # Session management (manager, session, http_handler)
│   └── sessionmanager/        # Client for appservice to talk to session manager
├── systemd/                  # Systemd units and install scripts
├── config.yaml.example
├── SPEC.md
└── README.md
```

## Security Considerations

1. **API Key Authentication**: Session Manager supports optional API key
2. **Path Validation**: Appservice validates directory paths
3. **Process Isolation**: Each pi runs in separate subprocess
4. **Environment Variables**: Sensitive config via environment variables

## License

AGPL-3.0
