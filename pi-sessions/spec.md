# Pi Sessions Matrix Appservice - Specification

## Project Name
**pi-matrix-bridge** - A Matrix appservice that bridges pi sessions to Matrix rooms

## Version
1.0.0

## Date Created
2026-04-14

## Status
Draft

---

## Problem Statement

We need a way for multiple pi sessions (running in different directories) to communicate through Matrix. Each pi session should:
1. Register with the appservice via a streaming socket
2. Create and manage Matrix rooms named after their working directory
3. Send/receive messages between Matrix and the pi sessions

Currently, pi sessions are isolated and have no unified communication channel. This bridge will enable:
- Centralized logging and monitoring of pi sessions
- Cross-session communication via Matrix
- Integration with existing Matrix clients (Element, etc.)

---

## Goals & Success Criteria

### Primary Goals
1. **Appservice Registration**: Register with the Matrix homeserver as a standard appservice
2. **Session Management**: Support multiple simultaneous pi session connections via WebSocket
3. **Room Creation**: Automatically create Matrix rooms named after each session's working directory
4. **Bidirectional Messaging**: Pass messages between Matrix rooms and connected pi sessions
5. **Pi Extension**: Create a TypeScript extension that pi sessions use to connect and send data
6. **Typing Indicators**: Show when the agent is working in the Matrix room
7. **Command Support**: Allow Matrix users to send commands like /new, /tree, /fork to pi

### Success Metrics
- Multiple pi sessions can connect simultaneously
- Each session's room is created with the directory name as the room name
- Messages from Matrix are forwarded to the correct session
- Messages from sessions appear in the correct Matrix room
- Connection handles reconnection gracefully

### Non-Goals
- Authentication system (sessions identify by directory path only)
- End-to-end encryption support
- File/media transfer (text messages only for v1)
- User management beyond session identification

---

## Functional Requirements

### User Stories
1. As a **pi session**, I want to connect to the appservice via WebSocket so I can send/receive Matrix messages
2. As a **pi session**, I want my Matrix room to be named after my working directory so it's easy to identify
3. As a **Matrix user**, I want to join a pi session's room to communicate with that session
4. As a **Matrix user**, I want to see typing indicators when the agent is working
5. As a **Matrix user**, I want to control pi sessions using commands like /new, /tree, /fork
6. As an **admin**, I want to see which sessions are connected and their status

### Core Features

#### 1. Appservice Infrastructure
- Standard Matrix appservice registration (registration.yaml)
- HTTP endpoint for appservice callbacks from Matrix server
- WebSocket server for pi session connections
- Configuration via config.yaml

#### 2. Session Management
- WebSocket-based session connections
- Session identified by working directory path
- Session state tracking (connected/disconnected)
- Automatic room creation on session connect
- Room cleanup on session disconnect (optional)

#### 3. Room Management
- Create Matrix room with directory-based naming
- Track room-to-session mapping
- Handle Matrix events (messages, joins, leaves)

#### 4. Message Bridge
- Matrix → Session: Forward messages to appropriate session
- Session → Matrix: Send messages to appropriate room
- Support text messages with basic formatting

#### 5. Pi Extension (pi-side client)
- Connect to appservice WebSocket
- Authenticate with session identifier (directory path)
- Send messages to Matrix
- Receive messages from Matrix
- Handle connection state
- Show typing indicators when agent is working
- Forward tool execution updates to Matrix
- Route Matrix commands to pi (new, tree, fork, etc.)

#### 6. Typing Indicators
- Track agent activity (turns, tool execution)
- Send typing indicators to Matrix when agent is working
- Show current tool being executed
- Display streaming status updates

#### 7. Command Routing
- Parse commands from Matrix messages (/new, /tree, /fork, etc.)
- Route commands to pi as user messages
- Provide help and status commands

### Data Flow

```
┌─────────────┐     WebSocket      ┌─────────────────┐     Matrix Protocol     ┌─────────────┐
│  Pi Session │◄──────────────────►│  Appservice      │◄───────────────────────►│ Matrix HS    │
│  (Directory)│                    │  (pi-matrix)     │                          │             │
└─────────────┘                    └────────┬─────────┘                          └─────────────┘
                                          │
                                  ┌───────┴────────┐
                                  │  Session Mgmt  │
                                  │  Room Mgmt     │
                                  └────────┬───────┘
                                          │
                                  ┌───────┴────────┐
                                  │  Message Queue │
                                  └────────────────┘
```

### User Interactions

1. **Pi Session Startup**
   - Start pi with the extension
   - Extension connects to appservice WebSocket
   - Appservice creates room named after working directory
   - Session receives confirmation

2. **Matrix User Joins Session Room**
   - User joins room (e.g., #/data/projects/alpha:server.com)
   - Appservice allows join
   - User sees session status

3. **Messaging**
   - Matrix user sends message → Appservice → Session
   - Session sends message → Appservice → Matrix room

---

## Technical Requirements

### Languages
- **Go 1.25+** for the appservice
- **Go** for the pi extension (integrates with existing pi codebase)

### Frameworks & Libraries
- `maunium.net/go/mautrix` (v0.26+) - Matrix client/bridge library
- `gorilla/websocket` - WebSocket handling
- `gopkg.in/yaml.v3` - Configuration parsing

### Architecture Patterns
- Bridge pattern: Separates Matrix protocol from session management
- Event-driven: Handles Matrix events and session events
- Connection pooling: Manages multiple WebSocket connections

### Development Style
- TDD with unit tests
- Modular design with clear interfaces

---

## Non-Functional Requirements

### Performance
- Support 10+ concurrent session connections
- Message latency < 500ms
- WebSocket connection timeout handling

### Security
- Appservice token verification
- Session identifier validation
- Rate limiting on messages

### Scalability
- Stateless session management (can scale horizontally)
- In-memory session tracking (can be extended to Redis)

### Reliability
- Auto-reconnect for sessions
- Graceful shutdown handling
- Message delivery confirmation

---

## Data Requirements

### Configuration Data (config.yaml)
```yaml
homeserver:
  address: http://localhost:8008
  domain: localhost

appservice:
  id: pi-sessions
  localpart: pi-bridge
  port: 29318

websocket:
  host: 0.0.0.0
  port: 8434
```

### Session State
- Session ID (directory path)
- WebSocket connection
- Associated Matrix room ID
- Connection timestamp
- Last activity timestamp

### Room State
- Room ID
- Session ID (owner)
- Room name
- Creation timestamp

---

## API & Integration Requirements

### Internal APIs

#### WebSocket Protocol (Session ↔ Appservice)
```json
// Session connects
{"type": "register", "session_id": "/data/projects/alpha"}

// Session sends message
{"type": "message", "content": "Hello from pi!", "room_id": "!abc:server.com"}

// Appservice sends message to session
{"type": "message", "sender": "@user:server.com", "content": "Hello pi!"}

// Appservice notifies session
{"type": "event", "event": "user_joined", "user": "@user:server.com"}
```

### External APIs

#### Matrix Appservice API
- `PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}` - Send message
- `PUT /_matrix/client/v3/rooms/{roomId}/state/{eventType}` - Update room state
- `GET /_matrix/client/v3/rooms/{roomId}/members` - Get room members

---

## Testing Requirements

### Unit Tests
- Session management logic
- Room creation logic
- Message routing
- Configuration parsing

### Integration Tests
- WebSocket connection handling
- Matrix event processing
- Multi-session scenarios

---

## Deployment Requirements

### Environments
- Development: Local Matrix server (Synapse)
- Production: Configured Matrix homeserver

### Infrastructure
- Single binary deployment
- Docker container support
- Environment-based configuration

### Monitoring
- Structured logging (JSON)
- Connection status logging
- Error tracking

---

## Dependencies & Constraints

### External Dependencies
- Matrix homeserver (Synapse or Dendrite)
- Go 1.25+ toolchain

### Constraints
- Pi extension must be compatible with existing pi codebase
- Must work with existing Matrix infrastructure

---

## Risks & Mitigation

### Technical Risks
1. **WebSocket scalability**: Sessions may disconnect unexpectedly
   - Mitigation: Implement heartbeat/ping mechanism

2. **Room name conflicts**: Same directory path on different servers
   - Mitigation: Include homeserver domain in room naming

3. **Message ordering**: Messages may arrive out of order
   - Mitigation: Include timestamps, implement ordering if needed

### Resource Risks
1. **Too many rooms**: Uncontrolled session proliferation
   - Mitigation: Admin controls for room limits

---

## Out of Scope for v1
- Multi-user DMs
- File/image attachments
- End-to-end encryption
- User authentication
- Backfill/history
- Threading
