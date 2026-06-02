# Plan: Replace session manager with forge API

## Goal

Replace the matrix app service's HTTP communication with `pi-session-manager` (a
custom Go service) with HTTP communication with the forge REST API. This
consolidates session management, audit logging, and LLM orchestration into a
single durable backend instead of two services.

## Background

Today the architecture is:

```
matrix homeserver
   │
   ▼
pi-matrix (appservice, on matrix server)
   │
   │ HTTP + SSE
   ▼
pi-session-manager (custom Go service, on user's machine)
   │
   │ stdin/stdout
   ▼
   pi (Node.js)
```

After this change, it becomes:

```
matrix homeserver
   │
   ▼
pi-matrix (appservice, on matrix server)
   │
   │ HTTP
   ▼
forge-api (Rust, on the user's machine)
   │
   │ stdin/stdout
   ▼
   pi (Node.js) + forge-tools extension
```

The session manager process and the appservice both spawn pi subprocesses
locally; forge already does this with one persistent pi per session. The forge
audit log (PostgreSQL `messages` table) is the new source of truth.

## API mapping

| Old (session manager)                                  | New (forge)                                                                                     |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| `POST /sessions` `{directory, user_id, machine_name}`  | `POST /profiles` (if needed) then `POST /sessions` `{profile_id, title}`                       |
| `GET /sessions/{id}`                                   | `GET /sessions/{id}` (same shape)                                                               |
| `GET /sessions`                                        | `GET /sessions` (same)                                                                          |
| `DELETE /sessions/{id}`                                | `DELETE /sessions/delete?id={id}`                                                               |
| `POST /sessions/{id}/prompt` `{message, streamingBehavior}` | `POST /messages` `{session_id, content}`                                                  |
| `GET /events` (SSE)                                    | `GET /sessions/{id}/events?since=<seq>` (SSE)                                                    |
| `streamingBehavior: "steer"`                           | (no native support; we just send another `POST /messages` — the LLM gets the new intent)        |

Forge auth: `X-API-Key: <key>` on every request.

## Per-user profile caching

Forge binds a session to a profile, and the profile owns the working dir.
There's no way to override the working dir at session-create time. So we have
to ensure the right profile exists for each (matrix user, working dir).

The appservice already has a SQLite store. We'll add a `forge_profile` table:

```sql
CREATE TABLE forge_profile (
    user_id      TEXT NOT NULL,
    working_dir  TEXT NOT NULL,
    profile_id   TEXT NOT NULL,    -- forge uuid
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (user_id, working_dir)
);
```

When `/start <path>` is called, the appservice:

1. Looks up `(user_id, working_dir)` in `forge_profile`. If found, reuse the
   profile_id.
2. Otherwise, calls `POST /profiles` with the working dir plus a default
   provider/model (configurable), saves the new id, and uses it.
3. Calls `POST /sessions` with that profile_id.

The default profile template (provider, model, base_url, api_key, system_prompt,
tools) comes from a new top-level `forge.default_profile` block in the config.
If a user wants different per-directory config, they can later edit the profile
in the forge DB or via the CLI.

## Event reconstruction from forge SSE

forge exposes a Server-Sent Events endpoint at
`GET /sessions/{id}/events?since=<seq>`. The appservice subscribes
to it (one connection per active session) and translates the
events into matrix events. On connect the server replays any
rows with `sequence > since` (catch-up), then forwards new rows
in real time. The connection auto-reconnects with exponential
backoff.

| Forge SSE event                                            | Matrix event                                                            |
| ---------------------------------------------------------- | ----------------------------------------------------------------------- |
| `message` with `role=user`                                 | `typing_start` (we know the agent will work)                            |
| `message` with `role=assistant, content`                  | `message` with the content                                              |
| `message` with `role=assistant, tool_call_id`              | `tool_start` (tool_name)                                                |
| `message` with `role=tool, tool_call_id`                   | `tool_end` (tool_name, success derived from `tool_output->>success`)    |
| `turn_ended`                                                | `typing_stop` (immediate)                                              |
| (no events for `typing_quiet_ms`)                          | `typing_stop` (fallback for crash)                                      |

We track a "high-water mark" sequence number per session and only
emit events for rows with `sequence > last_seen`. State is held
in memory; on startup the consumer seeds `last_seen` from the
current max sequence (via `GET /messages`), so the first SSE
replay only delivers genuinely new rows.

## `/new` and `/steer`

- `/new` deletes the current session and creates a new one against the same
  profile. The matrix room stays the same; we just rebind the session id.
- `/steer` is best-effort: we just send the message via `POST /messages`. Forge
  doesn't have an explicit "abort current task" verb, but the new prompt is
  appended to the LLM context.

## Multi-machine support

Removed. The `session_managers:` block in config is replaced by a single
`forge:` block with `url` and `api_key`. If a deployment really needs multiple
forge instances, run multiple appservice processes pointed at different homes.

## Configuration diff

```yaml
# OLD
session_managers:
  managers:
    - name: "desktop"
      url: http://192.168.1.10:19081
      api_key: ""

# NEW
forge:
  url: http://localhost:8080
  api_key: "sk_forge_..."
  poll_interval_ms: 1000         # optional, default 1000
  typing_quiet_ms: 3000          # optional, default 3000
  default_profile:
    provider: anthropic
    model: claude-sonnet-4-20250514
    base_url: null               # optional
    api_key: null                # optional, falls back to env
    system_prompt: "You are a helpful coding assistant."
    tools: [bash, read, write, edit]
```

`session_manager:` and `pi:` blocks are removed (the matrix appservice no
longer spawns pi directly).

## File changes

- **NEW** `pkg/forge/client.go` — typed Go client for the forge REST API
- **NEW** `pkg/forge/events.go` — message-poller that turns `messages` rows into
  appservice events
- **MODIFIED** `pkg/appservice/appservice.go` — switch from `sessionmanager.Client`
  to `forge.Client` + `forge.EventPoller`; drop machine-name logic
- **MODIFIED** `pkg/config/config.go` — replace `SessionManager`/`SessionManagers`
  blocks with `Forge` block
- **MODIFIED** `cmd/pi-matrix/main.go` — wire forge client
- **DELETED** `pkg/sessionmanager/` — replaced by `pkg/forge/`
- **UNCHANGED** `pkg/matrix/`, `pkg/store/` — store gets a new table; matrix
  client is unchanged
- **UPDATED** docs (SPEC.md, AGENTS.md, README.md, config example)

## Phases

### Phase 1: foundation
- [ ] Define `pkg/forge` types matching the forge API (Profile, Session, Message, etc.)
- [ ] Implement `pkg/forge.Client` with all the methods we need
- [ ] Add `forge_profile` table to the store and `SaveProfile`/`GetProfile` methods
- [ ] Update `config.go` with the new `forge:` block

### Phase 2: bridge
- [ ] Implement `pkg/forge.EventPoller` that polls `GET /messages` and emits
      `typing_start`/`typing_stop`/`message`/`tool_start`/`tool_end` events
- [ ] Add per-room high-water-mark tracking
- [ ] Replace the `SessionEvent` plumbing in `appservice.go` with the new
      poller
- [ ] Update `handleStartCommand` to look up / create the forge profile
- [ ] Update `handleNewCommand` to delete the old forge session and create
      a new one
- [ ] Drop `machine_name` from room names and event payloads

### Phase 3: testing
- [ ] Unit tests for `pkg/forge` client (httptest, success + error paths)
- [ ] Unit tests for the event poller (sequence numbers, idle detection)
- [ ] Build the binary and verify it starts cleanly against a stub forge
      server

### Phase 4: docs
- [ ] Update SPEC.md to describe the new architecture
- [ ] Update AGENTS.md
- [ ] Update README.md
- [ ] Update config.yaml.example
- [ ] Drop `pi-session-manager` references in deploy instructions

## Out of scope

- No forge schema changes (the `messages` table already has everything we need)
- No streaming endpoint (forge doesn't expose one for agent text)
- No edit/delete of an already-running session's profile (use forge CLI)
- No migration of existing local SQLite portal/session data — operator starts
  fresh
