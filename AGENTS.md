# Agent Guide - pi-matrix

This document provides essential information for AI agents working on this project.

## Project Overview

**pi-matrix** is a Matrix appservice that bridges the [forge](https://github.com/jbutlerdev/forge)
durable-agent backend to Matrix rooms. The appservice is the only Go process we run;
forge (a Rust service) owns the pi subprocesses, the message log, and the tool audit
trail.

## Architecture

```
┌──────────────────────────┐         ┌──────────────────────────┐
│ pi-matrix (Appservice)   │◄───────►│ forge-api (Rust)          │
│ (on matrix homeserver)   │  HTTP   │ (runs on user's machine)  │
│                          │         │                          │
│ - Matrix protocol        │         │ - spawns pi per session  │
│ - command handling       │         │ - persists message log   │
│ - polls forge for events │         │ - runs tools via ext     │
└──────────────────────────┘         └────────┬─────────────────┘
                                              │ stdin/stdout
                                              ▼
                                    ┌──────────────────────┐
                                    │ pi + forge-tools ext │
                                    └──────────────────────┘
```

The appservice no longer spawns pi or maintains its own session manager. It is a
pure client of forge.

## Key Files

| File | Purpose |
|------|---------|
| `pi-sessions/cmd/pi-matrix/main.go` | Entry point |
| `pi-sessions/cmd/sse-smoke/main.go` | Debug driver — exercises the `EventConsumer` against a real running forge. Builds to `sse-smoke`. |
| `pi-sessions/pkg/forge/` | forge REST client + SSE event consumer |
| `pi-sessions/pkg/appservice/` | Matrix <-> forge event translation |
| `pi-sessions/pkg/matrix/` | Matrix protocol client (mautrix) |
| `pi-sessions/pkg/store/` | SQLite portal + profile cache |
| `pi-sessions/pkg/config/` | YAML config loader |
| `SPEC.md` | Architecture specification |
| `AGENTS.md` | This file |
| `.env` | Sensitive configuration (IPs, credentials) |

## Configuration

Sensitive configuration (hosts, credentials) is in `.env`:
- **DO NOT** commit `.env` to version control
- **DO NOT** hardcode credentials in source code

The appservice config (`config.yaml` next to the binary) declares the forge URL
and API key. The forge URL is the host the appservice will poll for new message
rows.

## Development

### Building

```bash
cd /data/jbutler/git/mule-ai/matrix-appservice/pi-sessions

# Build appservice
go build -o pi-matrix ./cmd/pi-matrix
```

The `pi-session-manager` binary is no longer needed for normal operation and
is kept only for reference / disaster-recovery scenarios. The `cmd/pi-session-manager`
package still builds.

### Testing

```bash
# Run all tests
GOCACHE=/data/.gocache GOMODCACHE=/data/.gomodcache TMPDIR=/data/.tmp \
  go test ./... -v

# Run just the forge + appservice suites
GOCACHE=/data/.gocache GOMODCACHE=/data/.gomodcache TMPDIR=/data/.tmp \
  go test ./pkg/forge/... ./pkg/appservice/... -v
```

> **The repo's disk is usually full.** The Go toolchain needs ~5 GB of free
> space for the build cache. Set `GOCACHE`, `GOMODCACHE`, and `TMPDIR` to
> directories under `/data` to avoid the quota error.

## Deployment

The appservice is a single binary that runs on the Matrix homeserver.

```bash
# Build
cd /data/jbutler/git/mule-ai/matrix-appservice/pi-sessions
go build -o /data/.tmp/pi-matrix ./cmd/pi-matrix

# Deploy over SSH
sshpass -p 'drow22ap' scp /data/.tmp/pi-matrix root@10.10.199.186:/opt/pi-matrix/pi-matrix
sshpass -p 'drow22ap' ssh root@10.10.199.186 "systemctl restart pi-matrix"

# View logs
sshpass -p 'drow22ap' ssh root@10.10.199.186 "journalctl -u pi-matrix -f"
```

## Forge API Surface We Use

| Method | Path | Purpose |
|--------|------|---------|
| GET    | `/health` | Startup liveness check |
| GET    | `/profiles` | List profiles (used to find by working dir) |
| POST   | `/profiles` | Mint a profile for a (user, dir) pair |
| GET    | `/profiles/{id}` | (unused; we trust the create response) |
| POST   | `/sessions` | Open a new session against a profile |
| GET    | `/sessions/{id}` | Fetch a session (used by `/new`) |
| DELETE | `/sessions/delete?id={id}` | Close a session |
| GET    | `/sessions` | `/list` command |
| POST   | `/messages` | Forward a user prompt to forge |
| GET    | `/sessions/{id}/events?since=<seq>` | SSE stream of new message rows + turn_ended signals |

All requests carry `X-API-Key: <key>` from the `forge.api_key` config field.

## Event Reconstruction

forge exposes a Server-Sent Events endpoint at
`GET /sessions/{id}/events?since=<seq>`. The appservice
(`pkg/forge/events.go`) opens one SSE connection per active
session. On connect the server replays any rows with
`sequence > since`, then forwards new rows in real time. The
connection auto-reconnects with exponential backoff on transient
errors.

| forge event | matrix appservice event |
|-------------|-------------------------|
| SSE `message` with `role=user` | `typing_start` (we know the agent will work) |
| SSE `message` with `role=assistant, content` | `message` with the body |
| SSE `message` with `role=assistant, tool_call_id` | `tool_start` (tool name) |
| SSE `message` with `role=tool, tool_call_id` | `tool_end` (tool name, is_error derived from `tool_output->>success`) |
| SSE `turn_ended` | `typing_stop` (immediate) |
| (no events for `typing_quiet_ms`) | `typing_stop` (fallback for crash) |

## Per-User Profile Caching

forge binds a session to a profile, and a profile owns the working dir. There
is no way to override the working dir at session-create time. The appservice
mints one forge profile per working dir (cached in the SQLite `forge_profile`
table) so the model / system prompt / tools stay consistent across `/start`
calls for the same directory.

When a user does `/start /tmp/proj` for the first time:

1. The appservice checks `(user_id, /tmp/proj)` in the local SQLite cache.
2. On miss, it calls `POST /profiles` with the working dir plus a default
   template (configured in `forge.default_profile`).
3. The new profile id is cached and persisted.
4. The appservice calls `POST /sessions` against that profile id.

The next `/start /tmp/proj` (any user) reuses the profile id.

## Commands

### DM Commands (to bot)

| Command | Description |
|---------|-------------|
| `/start <path>` | Start a new session in the given directory |
| `/list` | List all active sessions |
| `/stop` | Stop your active session(s) |
| `/help` | Show help message |

### Room Commands (inside a session room)

| Command | Description |
|---------|-------------|
| `/new` | Reset the session with a fresh context |
| `/steer <msg>` | Send a steering message into the running session |
| `/help` | Show room commands |

## Troubleshooting

### Appservice can't reach forge

```bash
# From the appservice host:
curl $FORGE_URL/health
```

Look for `forge is not reachable at startup` in the journal. The appservice
warns at startup and continues; it will retry the call per request.

### Sessions don't appear in Matrix

1. The `forge_profile` SQLite table has the `(user, dir) -> profile_id` map.
   If the appservice was restarted, that table is reloaded from disk.
2. The `portal` table has the `session_id -> room_id` map. Same deal.
3. The poller needs to be tracking the session; we call `Track(sessionID)`
   on every `/start` and `/new`.

### `pi` doesn't reply

forge is responsible for that. Check forge's journal:

```bash
sudo journalctl -u forge-api -n 200
```

If pi is stalled, you'll see `pi timed out waiting for response after 60s`.

### Old `pi-session-manager` references

The session-manager binary and the `pkg/session/` package still exist in the
repo for historical reasons, but the appservice no longer talks to them. The
`session_manager:` and `session_managers:` blocks in `config.yaml` are
**ignored**. The only session backend is forge.
