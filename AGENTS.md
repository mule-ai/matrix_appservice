# Agent Guide - pi-matrix

This document provides essential information for AI agents working on this project.

## Project Overview

**pi-matrix** is a Matrix appservice that bridges [pi coding agent](https://github.com/mariozechner/pi-coding-agent) sessions to Matrix rooms. It consists of two services:

1. **pi-session-manager** - Runs locally, manages pi subprocesses
2. **pi-matrix** - Runs on the matrix homeserver, handles Matrix protocol

## Key Files

| File | Purpose |
|------|---------|
| `SPEC.md` | Architecture specification |
| `AGENTS.md` | This file |
| `.env` | Sensitive configuration (IP, credentials) |

## Architecture

```
┌─────────────────────────┐         ┌─────────────────────────┐
│   pi-matrix (Appservice)│◄───────►│ pi-session-manager       │
│   (on matrix homeserver) │ HTTP/SSE│ (runs locally)          │
└─────────────────────────┘         └─────────────────────────┘
```

- **Appservice** runs on the matrix homeserver
- **Session Manager** runs locally (same machine as user)

## Configuration

Sensitive configuration (hosts, credentials) is stored in `.env`:
- **DO NOT** commit `.env` to version control
- **DO NOT** hardcode credentials in source code

## Development

### Building

```bash
cd /data/jbutler/git/mule-ai/matrix-appservice/pi-sessions

# Build session manager
go build -o pi-session-manager ./cmd/pi-session-manager

# Build appservice
go build -o pi-matrix ./cmd/pi-matrix
```

### Testing

```bash
# Run all tests
go test ./... -v

# Run specific package tests
go test ./pkg/session/... -v
```

## Deployment

### Session Manager (Local)

The session manager runs on the local machine:

```bash
# Deploy binary
systemctl stop pi-session-manager
cp pi-session-manager /usr/local/bin/pi-session-manager
systemctl start pi-session-manager

# View logs
journalctl -u pi-session-manager -f
```

### Appservice (Matrix Homeserver)

The appservice runs on the matrix homeserver. more information is in .env

```bash
# Deploy binary (via SSH)
sshpass -p 'drow22ap' scp pi-matrix root@10.10.199.186:/opt/pi-matrix/pi-matrix
sshpass -p 'drow22ap' ssh root@10.10.199.186 "systemctl restart pi-matrix"

# View logs (via SSH)
sshpass -p 'drow22ap' ssh root@10.10.199.186 "journalctl -u pi-matrix -f"
```

## Session Manager API

The session manager exposes an HTTP API:

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| GET | /sessions | List all sessions |
| POST | /sessions | Create new session |
| GET | /sessions/{id} | Get session info |
| DELETE | /sessions/{id} | Delete session |
| POST | /sessions/{id}/prompt | Send prompt to session |
| GET | /events | SSE event stream |

### Example Session Creation

```bash
curl -X POST http://127.0.0.1:19081/sessions \
  -H "Content-Type: application/json" \
  -d '{"directory": "/data/jbutler/git/project", "user_id": "@jbutler:matrix.butler.ooo"}'
```

### Example Send Prompt

```bash
curl -X POST http://127.0.0.1:19081/sessions/{session-id}/prompt \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello, world!"}'
```

## SSE Events

The session manager streams events via SSE:

```json
{"type": "typing_start", "session_id": "..."}
{"type": "typing_stop", "session_id": "..."}
{"type": "message", "session_id": "...", "content": "..."}
{"type": "tool_start", "session_id": "...", "tool_name": "bash"}
{"type": "tool_end", "session_id": "...", "tool_name": "bash", "is_error": false}
```

## Troubleshooting

### Session Manager Not Responding

```bash
# Check health
curl http://127.0.0.1:19081/health

# Check sessions
curl http://127.0.0.1:19081/sessions

# View logs
journalctl -u pi-session-manager -n 100
```

### Appservice Not Receiving Events

1. Check session manager is broadcasting (logs show "broadcasting event")
2. Check appservice has SSE connection to session manager
3. Check session IDs match between services

```bash
# From appservice host, check session manager
curl http://10.10.199.96:19081/sessions
```

### Session ID Mismatch

If events are received but not delivered to Matrix rooms, the session ID in the appservice may not match the session manager. This can happen if a session is restarted.

Solution: The appservice maintains a mapping of Matrix rooms to session IDs. When a session restarts, the old session is deleted and a new one created.

## Commands

### DM Commands (to bot)

| Command | Description |
|---------|-------------|
| `/start <path>` | Start a new session in the specified directory |
| `/list` | List all active sessions |
| `/stop` | Stop your active session |
| `/help` | Show help message |

## Key Technical Details

### pi RPC Mode

pi runs in RPC mode (`pi --mode rpc`):
- Processes a single prompt then exits
- Communication via stdin/stdout
- Events streamed to stderr

### Session Lifecycle

1. Session created → state `pending`
2. pi starts → state `running`
3. pi exits after prompt → state `stopped`
4. Next prompt → pi restarts automatically

### Event Accumulation

`text_delta` events accumulate in memory during streaming. The complete text is only emitted as a `message` event when `text_end` is received. This prevents partial messages from being sent to Matrix.
