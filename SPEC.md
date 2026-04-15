# pi-matrix - Matrix Appservice for pi Sessions

## Version
2.1.0

## Status
Active

---

## Overview

**pi-matrix** is a distributed Matrix appservice that bridges [pi coding agent](https://github.com/mariozechner/pi-coding-agent) sessions to Matrix rooms. Users can create and interact with pi sessions directly from Matrix by sending DMs to the appservice bot.

## Key Features

- **Tool Call Visibility**: Users see when pi is running tools (bash, read, write, etc.) with timing information
- **Multiple Sessions**: Support for multiple simultaneous sessions across different directories
- **Session Reset**: `/new` command to reset a session with clean context
- **Continuous Conversation**: Same session maintains context across messages

---

## Architecture

The system uses a distributed architecture with two separate services:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    pi-matrix (Appservice)                                   │
│                                                                            │
│  - Lives centrally (single instance, runs on the matrix homeserver)         │
│  - Handles Matrix protocol (DMs, rooms, events)                            │
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

---

## Multiple Sessions

The system supports **multiple simultaneous sessions across different directories**. Each session:

1. Has its own dedicated Matrix room
2. Runs pi in its own subprocess
3. Maintains independent conversation context
4. Can be reset independently with `/new`

**Example setup:**
```
Session 1: /data/jbutler/git/jbutlerdev  →  Room: "Pi Session: jbutlerdev"
Session 2: /data/jbutler/git/mule-ai     →  Room: "Pi Session: mule-ai"
```

Users can switch between projects by joining different rooms.

---

## Tool Call Visibility

When pi executes a tool (bash command, file read, etc.), users see it in the Matrix room:

**Messages sent to room:**
```
🔧 Running bash...
❌ bash failed   (on error)
✅ bash completed  (on success, implicit - next message starts)
```

This helps users:
- Understand what pi is doing during "thinking" periods
- See the delay between sending a message and getting a response
- Verify correctness of responses by seeing what commands were run
- Debug issues by seeing tool execution results

---

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

**Commands (via Room Message)**:
| Command | Description |
|---------|-------------|
| `/new` | Reset session with clean context |
| `/help` | Show room commands |

**API Endpoints (consumed by appservice)**:
- `POST /sessions` - Create session
- `DELETE /sessions/{id}` - Delete session
- `POST /sessions/{id}/prompt` - Send prompt
- `GET /sessions` - List sessions
- `GET /sessions/{id}` - Get session info
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

### 3. Tool Execution Visibility

```
User: "List all files"
    → Room message sent

pi: thinking... → typing_start event
pi: 🔧 Running bash... → tool_start event (ls -la)
pi: [tool executes]
pi: ✅ bash completed → tool_end event
pi: Here's the file list... → message event
    → Sent to room
```

### 4. Session Reset with /new

```
User ──Room: "/new"──► Appservice
                          │
                          ▼
                    Delete old session
                    Create new session
                    (same directory)
                          │
                          ▼
                    "Session reset" message
```

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

### Automatic Restart

When a prompt is sent to a stopped session:

```
SendPrompt → session.state == stopped → Start() → pi restarts → prompt sent
```

This allows sessions to be maintained across multiple conversations without keeping pi running continuously.

### Session Context

- **Conversation history** is maintained within a session across multiple messages
- **`/new` command** clears context by creating a fresh session in the same directory
- Sessions persist even when pi exits (pi restarts automatically on next prompt)

---

## Event Accumulation

Text responses are accumulated during streaming to avoid sending partial messages:

1. `text_delta` events accumulate in memory
2. When `text_end` is received, the complete text is sent as a single `message` event
3. This prevents Matrix from showing incomplete/streaming responses

---

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

---

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

---

## Security Considerations

1. **API Key Authentication**: Session Manager supports optional API key
2. **Path Validation**: Appservice validates directory paths
3. **Process Isolation**: Each pi runs in separate subprocess
4. **Environment Variables**: Sensitive config via environment variables

---

## License

AGPL-3.0
