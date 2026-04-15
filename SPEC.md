# pi-matrix - Matrix Appservice for pi Sessions

## Version
3.0.0

## Status
Active

---

## Overview

**pi-matrix** is a distributed Matrix appservice that bridges [pi coding agent](https://github.com/mariozechner/pi-coding-agent) sessions to Matrix rooms. Users can create and interact with pi sessions directly from Matrix by sending DMs to the appservice bot.

## Key Features

- **Tool Call Visibility**: Users see when pi is running tools (bash, read, write, etc.) with timing information
- **Multiple Sessions**: Support for multiple simultaneous sessions across different directories
- **Multi-Machine Support**: Run session managers on different machines, control from single appservice
- **Session Reset**: `/new` command to reset a session with clean context
- **Continuous Conversation**: Same session maintains context across messages
- **Steering**: Interrupt the agent mid-task with `/steer <message>` to redirect it

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
│  - Routes messages to/from Session Managers (multiple)                     │
│  - Receives events via SSE from all Session Managers                       │
│                                                                            │
└────────┬────────────────────────────────┬───────────────────────────────┬────┘
         │ SSE                            │ SSE                           │ SSE
         ▼                                ▼                               ▼
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│ Session Manager  │         │ Session Manager  │         │ Session Manager  │
│ (machine: desktop)│         │ (machine: laptop)│         │ (machine: server)│
└─────────────────┘         └─────────────────┘         └─────────────────┘
```

### Why Two Services?

- **Session Manager** runs locally because it spawns `pi` subprocesses that need access to the user's files, git repositories, and environment.
- **Appservice** runs on the Matrix homeserver because it needs to communicate with the Matrix server directly with minimal latency.
- Communication between them uses HTTP with SSE for event streaming.

### Multi-Machine Support

Multiple session manager instances can connect to a single appservice:

- Each session manager identifies itself with a `machine_name`
- Users specify the machine when starting a session: `/start <machine> <path>`
- Sessions are distributed across machines based on user selection
- Events from all managers are routed to the correct Matrix rooms

---

## Multiple Sessions

The system supports **multiple simultaneous sessions across different directories**. Each session:

1. Has its own dedicated Matrix room
2. Runs pi in its own subprocess
3. Maintains independent conversation context
4. Can be reset independently with `/new`
5. Can run on any configured machine

**Example setup:**
```
Session 1: desktop:/data/jbutler/git/jbutlerdev  →  Room: "Pi: desktop: jbutlerdev"
Session 2: laptop:/data/jbutler/git/mule-ai     →  Room: "Pi: laptop: mule-ai"
Session 3: server:/opt/deployment                →  Room: "Pi: server: deployment"
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
- Message routing to Session Managers
- Receiving events via SSE from all managers
- Routing events to correct rooms based on session ID

**Commands (via DM)**:
| Command | Description |
|---------|-------------|
| `/start <machine> <path>` | Create new session on specified machine |
| `/start <path>` | Create new session (uses first available machine) |
| `/list` | List active sessions (shows machine, directory, user) |
| `/stop` | Stop user's active session |
| `/help` | Show help |

**Commands (via Room Message)**:
| Command | Description |
|---------|-------------|
| `/new` | Reset session with clean context |
| `/help` | Show room commands |

### Session Manager (`pi-session-manager`)

**Location**: Runs locally (one per machine)

**Responsibilities**:
- Spawn and manage pi subprocesses
- Handle RPC communication with pi
- Stream events back to Appservice
- Session lifecycle (timeouts, cleanup)
- Identify itself by machine name

**Configuration**:
```yaml
session_manager:
  host: 0.0.0.0
  port: 19081
  machine_name: "desktop"  # Identifies this machine
  pi_path: /usr/local/bin/pi
```

---

## User Flow

### 1. User Creates Session via DM

```
User ──DM: "/start desktop /home/user/projects/myapp"──► Appservice
                                                    │
                                                    ▼
                                    ┌──────────────────────┐
                                    │ Appservice routes to │
                                    │ "desktop" manager    │
                                    └──────────────────────┘
                                                    │
                                                    ▼
                                    ┌──────────────────────┐
                                    │ Desktop Session Mgr  │
                                    │ Spawns pi subprocess │
                                    └──────────────────────┘
                                                    │
                                                    ▼
                                    ┌──────────────────────┐
                                    │ Appservice creates   │
                                    │ Matrix room:         │
                                    │ "Pi: desktop: myapp" │
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
  machine_name: "desktop"  # NEW: Identifies this machine
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

# Multiple session managers (NEW)
session_managers:
  - name: "desktop"
    url: http://192.168.1.10:19081
    api_key: ""
  - name: "laptop"
    url: http://192.168.1.20:19081
    api_key: ""
  - name: "server"
    url: http://server.example.com:19081
    api_key: ""

# For backwards compatibility, single manager still works:
# session_manager:
#   url: http://localhost:19081
#   machine_name: "default"
```

---

## API Changes

### Session Creation Request (NEW)
```json
{
    "directory": "/path/to/project",
    "user_id": "@user:matrix.example.com",
    "machine_name": "desktop"
}
```

### Session Info Response (UPDATED)
```json
{
    "id": "uuid",
    "directory": "/path/to/project",
    "user_id": "@user:matrix.example.com",
    "machine_name": "desktop",
    "state": "running"
}
```

### SSE Events (UPDATED)
```json
{"type": "message", "session_id": "...", "machine_name": "desktop", "content": "..."}
{"type": "tool_start", "session_id": "...", "machine_name": "desktop", "tool_name": "bash"}
```

---

## Deployment

### Systemd Services

**Session Manager** (on each machine):
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
│   ├── config/                # Configuration (with multi-manager support)
│   ├── matrix/                # Matrix client (rooms, typing, etc.)
│   ├── session/               # Session management
│   ├── sessionmanager/        # Multi-manager client
│   └── store/                 # Persistence
├── docs/
│   └── MULTI_MANAGER_PLAN.md
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
5. **Machine Authentication**: Appservice validates machine names against configured list

---

## Changelog

### 3.0.0 - Multi-Machine Support
- Added `machine_name` to session manager config
- Appservice supports multiple session managers
- New `/start <machine> <path>` command syntax
- Room names include machine name
- Events include `machine_name` field
- Backwards compatible with single manager config

### 2.1.0 - Previous
- Markdown rendering improvements
- UTF-8 corruption fix
- Session reset command improvements

---

## License

AGPL-3.0
