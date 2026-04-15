# Multi-Session-Manager Support Plan

## Overview

Allow multiple `pi-session-manager` instances to connect to a single `pi-matrix` appservice, enabling users to run sessions on different machines while interacting through the same Matrix interface.

## Motivation

- **Multiple machines**: Users with desktop, laptop, server can run session managers on each
- **Resource distribution**: Heavy workloads can run on more powerful machines
- **Isolation**: Different projects can run on different machines

## User-Facing Changes

### New `/start` Command Syntax
```
/start <machine_name> <directory>
```

**Examples:**
```
/start desktop /data/jbutler/git/project
/start laptop ~/work
/start server /opt/deployment
```

### Room Naming
Rooms will be named: `Pi: {machine_name}: {directory}`

**Examples:**
- `Pi: desktop: /data/jbutler/git/project`
- `Pi: laptop: /home/user/work`
- `Pi: server: /opt/deployment`

### Machine Name Configuration
Each session manager has a `machine_name` in its config:
```yaml
session_manager:
  machine_name: "desktop"  # NEW: identifies this machine
  host: 0.0.0.0
  port: 19081
```

---

## Architecture Changes

### Current Architecture
```
┌─────────────────────────┐
│   pi-matrix (Appservice)│
│   (single session mgr)   │
└───────────┬─────────────┘
            │ SSE
            ▼
┌─────────────────────────┐
│ pi-session-manager       │
│ (single instance)       │
└─────────────────────────┘
```

### New Architecture
```
┌─────────────────────────────────────────┐
│        pi-matrix (Appservice)            │
│   (manages multiple session managers)     │
└────────┬──────────────────────────────┬───┘
         │ SSE                         │ SSE
         ▼                             ▼
┌─────────────────┐         ┌─────────────────┐
│ Session Manager  │         │ Session Manager  │
│ (machine: desktop)│       │ (machine: laptop)│
└─────────────────┘         └─────────────────┘
```

---

## Data Model Changes

### Session Manager

#### Config (`pkg/config/config.go`)
```go
type SessionManagerConfig struct {
    // ... existing fields ...
    
    // NEW: Machine identifier
    MachineName string `yaml:"machine_name"`
}
```

#### Session (`pkg/session/session.go`)
```go
type Session struct {
    ID           string
    Directory    string
    UserID       string
    MachineName  string  // NEW: which machine this session runs on
    State        State
    // ... existing fields ...
}
```

### Appservice

#### Client Config (`pkg/sessionmanager/client.go`)
```go
// NEW: Configuration for a single session manager
type ManagerConfig struct {
    URL         string
    APIKey      string
    MachineName string
}

// Updated client config for multiple managers
type ClientConfig struct {
    Managers   []ManagerConfig  // NEW: List of managers
    Logger     *zerolog.Logger
}
```

#### Session Event (`pkg/sessionmanager/client.go`)
```go
type SessionEvent struct {
    Type        string `json:"type"`
    SessionID   string `json:"session_id"`
    MachineName string `json:"machine_name"`  // NEW: Include in events
    Content     string `json:"content,omitempty"`
    ToolName    string `json:"tool_name,omitempty"`
    IsError     bool   `json:"is_error,omitempty"`
}
```

---

## API Changes

### Session Manager HTTP API (unchanged)
All existing endpoints remain the same. The appservice client will route to the correct manager.

### Session Creation Request
```json
{
    "directory": "/path/to/project",
    "user_id": "@user:matrix.example.com",
    "machine_name": "desktop"  // NEW field
}
```

### Session Info Response
```json
{
    "id": "uuid",
    "directory": "/path/to/project",
    "user_id": "@user:matrix.example.com",
    "machine_name": "desktop",  // NEW field
    "state": "running"
}
```

### SSE Events
```json
{"type": "message", "session_id": "...", "machine_name": "desktop", "content": "..."}
{"type": "tool_start", "session_id": "...", "machine_name": "desktop", "tool_name": "bash"}
```

---

## Appservice Configuration

### Current Config
```yaml
session_manager:
  url: http://localhost:19081
  api_key: ""
```

### New Config (backwards compatible)
```yaml
# Option 1: Single manager (backwards compatible)
session_manager:
  url: http://localhost:19081
  api_key: ""
  machine_name: "default"

# Option 2: Multiple managers
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
```

---

## Implementation Phases

### Phase 1: Core Infrastructure
1. Add `machine_name` to session manager config
2. Add `machine_name` to Session struct
3. Pass `machine_name` when creating sessions
4. Include `machine_name` in all SSE events
5. Update session HTTP handler to accept/return machine_name

### Phase 2: Appservice Multi-Manager Client
1. Update `ClientConfig` to support multiple managers
2. Create manager registry/router
3. Route API calls to correct manager based on machine_name
4. Maintain SSE connections to all managers
5. Demultiplex events from all managers

### Phase 3: User-Facing Changes
1. Update `/start` command parser to accept `<machine_name> <directory>`
2. Update room naming to include machine name
3. Add validation for machine names (must be registered)
4. Update `/list` to show machine name for each session
5. Update help text

### Phase 4: Testing & Polish
1. Test with single manager (backwards compatibility)
2. Test with two managers on same machine
3. Test event routing
4. Test session lifecycle across managers
5. Update documentation

---

## Files to Modify

### Session Manager
| File | Changes |
|------|---------|
| `pkg/config/config.go` | Add `MachineName` to `SessionManagerConfig` |
| `pkg/session/session.go` | Add `MachineName` field, update constructors, include in events |
| `pkg/session/http_handler.go` | Accept/return `machine_name` in API |
| `cmd/pi-session-manager/main.go` | Load and pass `machine_name` |
| `cmd/pi-session-manager/config.yaml` | Document new field |

### Appservice
| File | Changes |
|------|---------|
| `pkg/sessionmanager/client.go` | Support multiple managers, include machine_name in events |
| `pkg/appservice/appservice.go` | Parse `/start` with machine name, route to correct manager |
| `pkg/config/config.go` | Add `session_managers` list config |
| `pkg/matrix/room.go` | Update room naming |
| `cmd/pi-matrix/main.go` | Initialize multi-manager client |
| `cmd/pi-matrix/config.yaml` | Document new config format |

---

## Backwards Compatibility

1. **Single manager config**: If `session_manager.url` is set but `session_managers` is not, use single manager mode
2. **Missing machine_name**: If `machine_name` not provided, use "default" as fallback
3. **Old `/start` syntax**: If only path provided (no machine name), use "default" machine

---

## Error Handling

1. **Unknown machine name**: "Unknown machine: {name}. Available: desktop, laptop, server"
2. **Manager unreachable**: "Machine {name} is not available. Try again later."
3. **All managers down**: "All session managers are unavailable. Please try again later."

---

## Migration Strategy

1. Add machine_name to config with default value
2. Update appservice to accept both old and new config formats
3. Phase rollout: appservice first, then session managers

---

## Acceptance Criteria

- [ ] Single session manager works (backwards compatible)
- [ ] `/start desktop /path` creates session on "desktop" manager
- [ ] `/start laptop /path` creates session on "laptop" manager  
- [ ] Room names include machine name
- [ ] Events from different managers are routed correctly
- [ ] `/list` shows machine name for each session
- [ ] Unknown machine names show helpful error
- [ ] Documentation updated
