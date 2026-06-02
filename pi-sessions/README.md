# Pi Matrix - Matrix Appservice for forge

A Matrix appservice that bridges [forge](https://github.com/jbutlerdev/forge)
durable-agent sessions to Matrix rooms. Users can create and interact with
agent sessions directly from Matrix.

## Architecture

```
┌──────────────────────────┐         ┌──────────────────────────┐
│   pi-matrix (Appservice) │◄───────►│   forge-api (Rust)        │
│                          │  HTTP   │                          │
│ - Matrix protocol        │         │ - Spawns pi per session  │
│ - DMs, rooms, events     │         │ - Persists message log   │
│ - Polls forge for events │         │ - Runs tools via ext     │
└──────────────────────────┘         └──────────┬───────────────┘
                                                │ stdin/stdout
                                                ▼
                                    ┌────────────────────────┐
                                    │  pi + forge-tools ext  │
                                    └────────────────────────┘
```

The appservice is a pure client of forge. forge owns the pi subprocesses,
the message log, and the tool audit trail.

## Services

### pi-matrix (Appservice)
- Handles Matrix protocol (DMs, rooms, events)
- Receives commands via DM: `/start`, `/list`, `/stop`, `/help`
- Creates and manages Matrix rooms for sessions
- Polls forge `GET /messages?session_id=...` once per second per active
  session; translates new rows into Matrix events
- Lives centrally (single instance, runs on the Matrix homeserver)

### forge (Rust)
- Spawns and manages one long-lived pi subprocess per session
- Persists every user/assistant/tool-call/tool-result row to PostgreSQL
- Runs the four tools (`bash`, `read`, `write`, `edit`) via the
  `forge-tools` extension
- Exposes a REST API the appservice consumes

## Quick Start

### Build

```bash
go build -o pi-matrix ./cmd/pi-matrix
```

The `pi-session-manager` binary is no longer needed for normal operation
and is kept in the tree only for reference.

### Run

```bash
./pi-matrix -c config.yaml
```

The first run exports a sample `config.yaml` to the path you pass (or to
`config.yaml` in the cwd). Edit the `forge:` block to point at your
local forge instance, then re-run.

### Install with systemd

```bash
sudo cp pi-matrix /opt/pi-matrix/pi-matrix
sudo cp config.yaml.example /etc/pi-matrix/config.yaml
$EDITOR /etc/pi-matrix/config.yaml
sudo systemctl restart pi-matrix
```

## Configuration

The full example is at `config.yaml.example`. The two blocks the
operator must set are `forge:` and `appservice:`:

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
  api_key: "sk_forge_..."
  default_profile:
    provider: anthropic
    model: claude-sonnet-4-20250514
    system_prompt: "You are a helpful coding assistant."
    tools: [bash, read, write, edit]
```

`forge.api_key` is sent as `X-API-Key` on every request to forge.

## Commands

### DM Commands (to `@pi-matrix:your.domain`)

| Command | Description |
|---------|-------------|
| `/start <path>` | Start a new session in the given directory |
| `/list` | List all active sessions |
| `/stop` | Stop your active session(s) |
| `/help` | Show help |

### Room Commands (inside a session room)

| Command | Description |
|---------|-------------|
| `/new` | Reset the session with a fresh context |
| `/steer <msg>` | Send a steering message into the running session |
| `/help` | Show room commands |

## Testing

```bash
# All tests
GOCACHE=/data/.gocache GOMODCACHE=/data/.gomodcache TMPDIR=/data/.tmp \
  go test ./... -v

# Just the forge + appservice suites (the new bits)
GOCACHE=/data/.gocache GOMODCACHE=/data/.gomodcache TMPDIR=/data/.tmp \
  go test ./pkg/forge/... ./pkg/appservice/... -v
```

> The repo's root filesystem is usually full. Set `GOCACHE`,
> `GOMODCACHE`, and `TMPDIR` to paths under `/data` to avoid disk-quota
> build errors.

## Project Structure

```
pi-sessions/
├── cmd/
│   └── pi-matrix/              # Appservice (the one binary we deploy)
├── pkg/
│   ├── appservice/             # Matrix <-> forge event translation
│   ├── config/                 # YAML config loader
│   ├── forge/                  # forge REST client + event poller
│   ├── matrix/                 # Matrix protocol client (mautrix)
│   ├── session/                # (legacy pi-session-manager, not used)
│   └── store/                  # SQLite portal + forge profile cache
├── config.yaml.example
├── SPEC.md                     # Architecture specification
└── README.md
```

## Development

```bash
# Build the appservice
go build -o pi-matrix ./cmd/pi-matrix

# Run the appservice in dev mode
./pi-matrix -c config.yaml

# Run with debug logging
RUST_LOG=debug ./pi-matrix -c config.yaml
```

(The `RUST_LOG` env var name is a holdover from the v3 architecture; the
Go appservice reads `logging.level` from the YAML.)

## License

AGPL-3.0
