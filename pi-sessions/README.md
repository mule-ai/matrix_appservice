# Pi Matrix - Matrix Appservice for pi Sessions

A distributed Matrix appservice that bridges pi coding agent sessions to Matrix rooms. Users can create and interact with pi sessions directly from Matrix.

## Architecture

```
┌─────────────────────────┐         ┌─────────────────────────┐
│   pi-matrix (Appservice)│◄───────►│ pi-session-manager       │
│                         │ HTTP/SSE │                         │
│ - Matrix protocol       │         │ - Spawns pi --mode rpc  │
│ - DMs, rooms, events   │         │ - Manages sessions      │
│ - Routes messages      │         │ - Streams events        │
└─────────────────────────┘         └─────────────────────────┘
```

## Services

### pi-matrix (Appservice)
- Handles Matrix protocol (DMs, rooms, events)
- Receives commands via DM: `/start`, `/list`, `/stop`, `/help`
- Creates and manages Matrix rooms for sessions
- Lives centrally (single instance)

### pi-session-manager
- Spawns and manages `pi --mode rpc` subprocesses
- Handles session lifecycle
- Streams events back to appservice via SSE
- Can be distributed/scaled (horizontally scalable)

## Quick Start

### Build

```bash
go build -o pi-session-manager ./cmd/pi-session-manager
go build -o pi-matrix ./cmd/pi-matrix
```

### Run Without systemd

```bash
# Terminal 1 - Start session manager
./pi-session-manager -c config.yaml

# Terminal 2 - Start appservice  
./pi-matrix -c config.yaml
```

### Install with systemd

```bash
cd systemd
chmod +x install.sh
sudo ./install.sh
sudo systemctl start pi-session-manager
sudo systemctl start pi-matrix
```

## Configuration

### Example config.yaml for appservice

```yaml
homeserver:
  address: http://localhost:8008
  domain: localhost

appservice:
  id: pi-matrix
  localpart: pi-matrix
  url: http://localhost:8080
  as_token: "${APPSERVICE_AS_TOKEN}"
  hs_token: "${APPSERVICE_HS_TOKEN}"

session_manager:
  url: http://localhost:8081
  api_key: "your-api-key"

bridge:
  room_name_prefix: "Pi Session"
  max_sessions: 10
```

## Commands (via DM to @pi-matrix)

| Command | Description |
|---------|-------------|
| `/start <path>` | Start a new session in the specified directory |
| `/list` | List all active sessions |
| `/stop` | Stop your active session |
| `/help` | Show help message |

## API Endpoints (Session Manager)

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| GET | /sessions | List all sessions |
| POST | /sessions | Create new session |
| GET | /sessions/{id} | Get session info |
| DELETE | /sessions/{id} | Delete session |
| POST | /sessions/{id}/prompt | Send prompt |
| GET | /events | SSE event stream |

## Testing

```bash
# Run all tests
go test ./... -v

# Run specific package tests
go test ./pkg/session/... -v
```

## Project Structure

```
pi-sessions/
├── cmd/
│   ├── pi-matrix/              # Appservice
│   └── pi-session-manager/     # Session manager
├── pkg/
│   ├── appservice/            # Appservice logic
│   ├── config/                # Configuration
│   ├── matrix/                # Matrix client
│   ├── session/               # Session management
│   └── sessionmanager/        # Appservice's session manager client
├── systemd/                  # Systemd units and install scripts
├── config.yaml.example
├── SPEC.md                   # Architecture specification
└── README.md
```

## Development

```bash
# Build both binaries
go build -o pi-session-manager ./cmd/pi-session-manager
go build -o pi-matrix ./cmd/pi-matrix

# Run tests
go test ./... -v

# Run with verbose output
RUST_LOG=debug ./pi-session-manager -c config.yaml
```

## License

AGPL-3.0
