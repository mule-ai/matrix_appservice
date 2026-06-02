# pi-matrix - Matrix Appservice for forge

## Version
4.0.0

## Status
Active

---

## Overview

**pi-matrix** is a Matrix appservice that bridges
[forge](https://github.com/jbutlerdev/forge) durable-agent sessions to Matrix
rooms. Users can drive forge pi sessions directly from Matrix by sending DMs
to the appservice bot.

## Key Features

- **Tool call visibility** — users see when the agent is running `bash` /
  `read` / `write` / `edit`, with timing and error status
- **Multiple sessions** — one session per Matrix room, each with its own
  working directory
- **Durable audit log** — every user prompt, assistant reply, tool call,
  and tool result lives in forge's `messages` table; the Matrix room is a
  rendering layer on top
- **Per-directory profiles** — the appservice mints a forge profile per
  working directory on first `/start`, so the model / system prompt / tools
  stay consistent across users
- **Session reset** — `/new` deletes the current forge session and mints a
  fresh one against the same profile
- **Steering** — `/steer <msg>` sends a steering message into the running
  session

## Architecture

The system is a two-tier architecture:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    pi-matrix (Appservice)                                  │
│                                                                            │
│  - Lives centrally (single instance, runs on the matrix homeserver)         │
│  - Handles Matrix protocol (DMs, rooms, events)                            │
│  - Receives DMs: /start, /list, /stop, /help                              │
│  - Creates Matrix rooms for sessions                                       │
│  - Routes messages to/from forge over HTTP                                  │
│  - Polls forge for new message rows; translates them into Matrix events    │
│                                                                            │
└────────┬───────────────────────────────────────────────────────────────────┘
         │ HTTP
         ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                    forge-api (Rust)                                        │
│                                                                            │
│  - Spawns and manages one long-lived pi subprocess per session             │
│  - Persists every user/assistant/tool-call/tool-result row to PostgreSQL   │
│  - Runs tools (bash/read/write/edit) via the forge-tools extension         │
│  - Exposes REST API: /profiles, /sessions, /messages, /tools/execute      │
│                                                                            │
└────────┬───────────────────────────────────────────────────────────────────┘
         │ stdin/stdout
         ▼
   ┌─────────────────┐
   │ pi (Node.js)    │
   │ + forge-tools   │
   └─────────────────┘
```

### Why this split?

- **forge** runs locally because it spawns `pi` subprocesses that need
  access to the user's files, git repositories, and environment.
- **pi-matrix** runs on the Matrix homeserver because it needs to
  communicate with the Matrix server directly with minimal latency.
- The split also gives us a durable audit log (forge's `messages` table)
  and lets forge evolve independently of Matrix.

### v3 → v4

The v3 appservice ran its own `pi-session-manager` service that spawned pi
subprocesses and streamed events over SSE. That service is now obsolete
because forge does the same job, more durably. v4 drops the session manager
and the appservice is now a pure client of forge.

---

## Tool Call Visibility

When forge records a tool call, users see it in the Matrix room:

```
🔧 Running bash...
❌ bash failed   (on error)
```

The poller (`pkg/forge/events.go`) maps forge's `messages` rows into
Matrix events:

| forge row                                              | Matrix event                                  |
| ------------------------------------------------------ | --------------------------------------------- |
| `role=user`                                            | `typing_start`                                |
| `role=assistant, content`                              | `message` (text)                              |
| `role=assistant, tool_call_id`                         | `tool_start` (with tool name)                 |
| `role=tool, tool_call_id`                              | `tool_end` (is_error from `tool_output.success`) |
| (idle for `typing_quiet_ms` after a turn)              | `typing_stop`                                 |

---

## Services

### Appservice (`pi-matrix`)

**Location**: runs on the matrix homeserver

**Responsibilities**:
- Matrix appservice protocol
- DM handling with commands
- Room creation and management
- Message routing to forge over HTTP
- Polling forge for new message rows; rendering them as Matrix events

**Commands (via DM)**:
| Command | Description |
|---------|-------------|
| `/start <path>` | Create new session in the given directory |
| `/list` | List active sessions |
| `/stop` | Stop your active session(s) |
| `/help` | Show help |

**Commands (via Room Message)**:
| Command | Description |
|---------|-------------|
| `/new` | Reset session with a fresh context |
| `/steer <msg>` | Send a steering message into the running session |
| `/help` | Show room commands |

### forge (`forge-api`)

**Location**: runs locally (one per machine that wants to expose sessions)

**Responsibilities**:
- Spawn and manage one long-lived pi subprocess per session
- Persist every conversation event to PostgreSQL
- Run tools (bash/read/write/edit) on behalf of the LLM
- Expose REST API consumed by the appservice

See the [forge docs](https://github.com/jbutlerdev/forge) for full details.

---

## User Flow

### 1. User Creates Session via DM

```
User ──DM: "/start /data/projects/myapp"──► Appservice
                                                │
                                                ▼
                                ┌──────────────────────────┐
                                │ Appservice mints or      │
                                │ reuses a forge profile   │
                                │ for (user, /data/...)    │
                                │ and POSTs /sessions      │
                                └──────────────────────────┘
                                                │
                                                ▼
                                ┌──────────────────────────┐
                                │ forge-api spawns pi in   │
                                │ the profile's working    │
                                │ dir and returns session  │
                                │ id                        │
                                └──────────────────────────┘
                                                │
                                                ▼
                                ┌──────────────────────────┐
                                │ Appservice creates a     │
                                │ Matrix room and invites  │
                                │ the user                 │
                                └──────────────────────────┘
```

### 2. User Interacts with Session

```
User ──Room Message──► Appservice ──POST /messages──► forge
                                                            │
                                                            ▼
                                                      ┌──────────┐
                                                      │   pi     │
                                                      └────┬─────┘
                                                           │ events written
                                                           ▼ to messages table
                                                      forge-api

Appservice ──polls GET /messages──► forge-api
                  │
                  ▼
            new rows → Matrix room events
```

---

## Session Lifecycle

forge owns the session lifecycle; the appservice is just a renderer. A
session is:

1. Created when the user does `/start <dir>` (appservice mints a profile
   + session, then opens a room)
2. Reused across many user messages — forge keeps pi alive for the life
   of the session
3. Reset on `/new` — appservice deletes the old session and creates a new
   one against the same profile (same working dir)
4. Closed on `/stop` — appservice deletes the session; the Matrix room
   stays but is detached

The appservice's portal cache (`session_id -> room_id`) is persisted to
SQLite, so restarts don't lose the mapping. The same is true of the
`(user, working_dir) -> profile_id` cache.

---

## Event Reconstruction from Polling

The appservice subscribes to `GET /sessions/{id}/events?since=<seq>`,
an SSE endpoint exposed by forge. On connect the server replays any
rows with `sequence > since` (catch-up) and then forwards new
rows in real time as the harness and tool executor write them.
The connection auto-reconnects with exponential backoff on
transient network errors. See
[forge's `docs/API.md` §Streaming](https://github.com/jbutlerdev/forge/blob/main/docs/API.md)
for the wire protocol.

---

## Configuration

### Appservice (`config.yaml`)

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

forge:
  url: http://localhost:8080
  api_key: ""
  poll_interval_ms: 1000
  typing_quiet_ms: 3000
  default_profile:
    provider: anthropic
    model: claude-sonnet-4-20250514
    base_url: ""
    api_key: ""
    system_prompt: "You are a helpful coding assistant."
    tools: [bash, read, write, edit]
```

`forge.url` is the URL of the local forge API. `forge.api_key` is sent on
every request as `X-API-Key`. The `default_profile` block is the
template for new forge profiles; the appservice fills in the working dir
per `/start`.

### forge (`/etc/forge/forge.env`)

forge is configured and deployed separately. See its docs.

---

## Changelog

### 4.0.0 — forge backend

- Drop the `pi-session-manager` service. The appservice now talks
  directly to forge over REST.
- Drop SSE event streaming. The appservice polls `GET /messages`.
- Drop the multi-machine `session_managers:` config block. The appservice
  talks to one forge.
- Drop the `machine_name` argument from `/start`. The directory alone
  determines the working dir.
- Drop `streamingBehavior: "steer"`. forge's `POST /messages` queues the
  next user message; the LLM sees it appended to context.
- Add per-(matrix user, working dir) forge profile cache (SQLite
  `forge_profile` table).
- Add forge event poller (`pkg/forge/events.go`) that turns `messages`
  rows into the same `SessionEvent` shape v3 used, so the appservice
  code is largely unchanged.

### 3.0.0 — Multi-machine support

- Appservice supports multiple session managers identified by machine name
- `/start <machine> <path>` command syntax
- `machine_name` field on sessions and events

### 2.x — Initial release

- Single session manager, single appservice, SSE events
- Tools rendered as Matrix notices

---

## License

AGPL-3.0
