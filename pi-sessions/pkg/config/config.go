// pi-matrix - A Matrix appservice for pi sessions via RPC mode.
// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the main configuration.
type Config struct {
	Homeserver       HomeserverConfig         `yaml:"homeserver"`
	Appservice       AppserviceConfig         `yaml:"appservice"`
	API              APIConfig                `yaml:"api"`
	Bridge           BridgeConfig             `yaml:"bridge"`
	SessionManager   SessionManagerConfig     `yaml:"session_manager"`
	SessionManagers SessionManagersConfig     `yaml:"session_managers"` // Multiple managers for appservice
	Forge            ForgeConfig              `yaml:"forge"`
	Pi               PiConfig                `yaml:"pi"`
	Database         DatabaseConfig           `yaml:"database"`
	Logging          LoggingConfig            `yaml:"logging"`
}

// HomeserverConfig contains the Matrix homeserver connection details.
type HomeserverConfig struct {
	Address    string `yaml:"address"`
	Domain     string `yaml:"domain"`
	AsyncMedia bool   `yaml:"async_media"`
}

// AppserviceConfig contains appservice-specific settings.
type AppserviceConfig struct {
	ID                       string `yaml:"id"`
	Localpart                string `yaml:"localpart"`
	URL                      string `yaml:"url"`
	RegistrationPath         string `yaml:"registration_path"`
	AutoGenerateRegistration bool   `yaml:"auto_generate_registration"`
	BotUsername              string `yaml:"bot_username"`
	EventWorkers             int    `yaml:"event_workers"`
	ASToken                  string `yaml:"as_token"`
	HSToken                  string `yaml:"hs_token"`
}

// APIConfig contains settings for the HTTP API server.
type APIConfig struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	APIKey      string `yaml:"api_key"`
	MaxQueueSize int   `yaml:"max_queue_size"`
}

// BridgeConfig contains bridge-specific settings.
type BridgeConfig struct {
	RoomNamePrefix        string `yaml:"room_name_prefix"`
	AutoCreateRooms       bool   `yaml:"auto_create_rooms"`
	DeleteRoomsOnExit     bool   `yaml:"delete_rooms_on_exit"`
	MaxSessions           int    `yaml:"max_sessions"`
	SessionTimeout        int    `yaml:"session_timeout"`
	RateLimitPerSecond    int    `yaml:"rate_limit_per_second"`
	RateLimitBurst        int    `yaml:"rate_limit_burst"`
}

// SessionManagerConfig contains settings for the session manager (used by both appservice and server).
type SessionManagerConfig struct {
	// Server settings (used by server)
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
	
	// Client settings (used by appservice)
	URL     string `yaml:"url"`
	
	// Auth
	APIKey  string `yaml:"api_key"`
	
	// Pi settings
	PiPath   string `yaml:"pi_path"`
	AgentDir string `yaml:"agent_dir"`
	
	// Limits
	MaxSessions    int `yaml:"max_sessions"`
	SessionTimeout int `yaml:"session_timeout"`
	
	// Persistence
	DataDir string `yaml:"data_dir"`
	
	// Identity (for server mode)
	MachineName string `yaml:"machine_name"`
}

// SessionManagersConfig contains multiple session manager configurations (for appservice).
type SessionManagersConfig struct {
	Managers []ManagerEndpointConfig `yaml:"managers"`
}

// ManagerEndpointConfig contains configuration for a single session manager endpoint.
type ManagerEndpointConfig struct {
	Name   string `yaml:"name"`
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

// ForgeConfig configures the connection to the forge REST API. The
// matrix appservice no longer runs its own session manager; sessions
// live in forge instead.
type ForgeConfig struct {
	// URL of the forge API (e.g. http://localhost:8080).
	URL string `yaml:"url"`

	// APIKey is sent as X-API-Key on every request. If empty, no
	// auth header is sent (only useful for unsecured dev instances).
	APIKey string `yaml:"api_key"`

	// ReconnectMinMs is the minimum backoff between SSE
	// reconnects when the stream fails. Default: 500.
	ReconnectMinMs int `yaml:"reconnect_min_ms"`

	// ReconnectMaxMs is the maximum backoff between SSE
	// reconnects. Default: 30000.
	ReconnectMaxMs int `yaml:"reconnect_max_ms"`

	// TypingQuietMs is the idle window after which a `typing_stop`
	// event is emitted if no new rows have arrived. Default: 3000.
	TypingQuietMs int `yaml:"typing_quiet_ms"`

	// DefaultProfile is the template the appservice uses when it
	// has to create a new forge profile for a (matrix user, working
	// dir) pair that hasn't been seen before. The forge profile is
	// the only place the working dir lives, so a new dir requires
	// a new profile.
	DefaultProfile ForgeDefaultProfile `yaml:"default_profile"`
}

// ForgeDefaultProfile is the template used to mint new forge
// profiles for matrix users. See ForgeConfig.DefaultProfile.
type ForgeDefaultProfile struct {
	Name         string   `yaml:"name"`
	Provider     string   `yaml:"provider"`
	Model        string   `yaml:"model"`
	BaseURL      string   `yaml:"base_url"`
	APIKey       string   `yaml:"api_key"`
	SystemPrompt string   `yaml:"system_prompt"`
	Tools        []string `yaml:"tools"`
}

// ReconnectMin returns the configured minimum SSE backoff,
// falling back to 500ms if not set.
func (c *ForgeConfig) ReconnectMin() time.Duration {
	if c.ReconnectMinMs <= 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(c.ReconnectMinMs) * time.Millisecond
}

// ReconnectMax returns the configured maximum SSE backoff,
// falling back to 30s if not set.
func (c *ForgeConfig) ReconnectMax() time.Duration {
	if c.ReconnectMaxMs <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.ReconnectMaxMs) * time.Millisecond
}

// TypingQuiet returns the configured quiet window, falling back to
// 3 seconds if not set.
func (c *ForgeConfig) TypingQuiet() time.Duration {
	if c.TypingQuietMs <= 0 {
		return 3 * time.Second
	}
	return time.Duration(c.TypingQuietMs) * time.Millisecond
}

// PiConfig contains settings for pi executable.
type PiConfig struct {
	Path     string `yaml:"path"`
	AgentDir string `yaml:"agent_dir"`
}

// DatabaseConfig contains database settings.
type DatabaseConfig struct {
	URL        string `yaml:"url"`
	PoolSize   int    `yaml:"pool_size"`
	MaxIdle    int    `yaml:"max_idle"`
	MaxLifetime int   `yaml:"max_lifetime"`
}

// LoggingConfig contains logging settings.
type LoggingConfig struct {
	Level   string   `yaml:"level"`
	Format  string   `yaml:"format"`
	Writers []string `yaml:"writers"`
}

// Normalize applies defaults and expands environment variables.
func (c *Config) Normalize() error {
	// Set defaults for homeserver (only required for appservice)
	if c.Homeserver.Address == "" {
		c.Homeserver.Address = "http://localhost:8008"
	}
	if c.Homeserver.Domain == "" {
		c.Homeserver.Domain = "localhost" // Default for session-manager
	}

	// Set defaults for appservice
	if c.Appservice.ID == "" {
		c.Appservice.ID = "pi-matrix"
	}
	if c.Appservice.Localpart == "" {
		c.Appservice.Localpart = "pi-matrix"
	}
	if c.Appservice.URL == "" {
		c.Appservice.URL = "http://localhost:29318"
	}
	if c.Appservice.RegistrationPath == "" {
		c.Appservice.RegistrationPath = "registration.yaml"
	}
	if c.Appservice.EventWorkers == 0 {
		c.Appservice.EventWorkers = 4
	}

	// Expand environment variables
	c.Appservice.ASToken = expandEnv(c.Appservice.ASToken)
	c.Appservice.HSToken = expandEnv(c.Appservice.HSToken)

	// Set defaults for API
	if c.API.Host == "" {
		c.API.Host = "0.0.0.0"
	}
	if c.API.Port == 0 {
		c.API.Port = 8080
	}
	if c.API.MaxQueueSize == 0 {
		c.API.MaxQueueSize = 100
	}

	// Set defaults for bridge
	if c.Bridge.RoomNamePrefix == "" {
		c.Bridge.RoomNamePrefix = "Pi Session"
	}
	if c.Bridge.RateLimitPerSecond == 0 {
		c.Bridge.RateLimitPerSecond = 10
	}
	if c.Bridge.RateLimitBurst == 0 {
		c.Bridge.RateLimitBurst = 20
	}

	// Set defaults for session manager
	if c.SessionManager.Port == 0 {
		c.SessionManager.Port = 8081
	}
	if c.SessionManager.Host == "" {
		c.SessionManager.Host = "0.0.0.0"
	}
	if c.SessionManager.URL == "" {
		c.SessionManager.URL = "http://localhost:8081"
	}
	if c.SessionManager.PiPath == "" {
		c.SessionManager.PiPath = "pi"
	}
	if c.SessionManager.MaxSessions == 0 {
		c.SessionManager.MaxSessions = 10
	}
	if c.SessionManager.DataDir == "" {
		c.SessionManager.DataDir = "/var/lib/pi-session-manager/sessions"
	}
	c.SessionManager.APIKey = expandEnv(c.SessionManager.APIKey)

	// Set defaults for forge. The default profile template falls back
	// to a minimal Anthropic config; the operator can override any
	// field in the YAML.
	if c.Forge.URL == "" {
		c.Forge.URL = "http://localhost:8080"
	}
	c.Forge.APIKey = expandEnv(c.Forge.APIKey)
	if c.Forge.DefaultProfile.Provider == "" {
		c.Forge.DefaultProfile.Provider = "anthropic"
	}
	if c.Forge.DefaultProfile.Model == "" {
		c.Forge.DefaultProfile.Model = "claude-sonnet-4-20250514"
	}
	if c.Forge.DefaultProfile.SystemPrompt == "" {
		c.Forge.DefaultProfile.SystemPrompt = "You are a helpful coding assistant."
	}
	if len(c.Forge.DefaultProfile.Tools) == 0 {
		c.Forge.DefaultProfile.Tools = []string{"bash", "read", "write", "edit"}
	}
	
	// Set default machine name to hostname if not specified
	if c.SessionManager.MachineName == "" {
		c.SessionManager.MachineName = getHostname()
	}
	
	// Backwards compatibility: if session_managers is not set but session_manager.url is,
	// create a default manager entry
	if len(c.SessionManagers.Managers) == 0 && c.SessionManager.URL != "" {
		machineName := c.SessionManager.MachineName
		if machineName == "" {
			machineName = "default"
		}
		c.SessionManagers.Managers = []ManagerEndpointConfig{
			{
				Name:   machineName,
				URL:    c.SessionManager.URL,
				APIKey: c.SessionManager.APIKey,
			},
		}
	}

	// Set defaults for pi
	if c.Pi.Path == "" {
		c.Pi.Path = "pi"
	}
	if c.Pi.AgentDir == "" {
		home, _ := os.UserHomeDir()
		c.Pi.AgentDir = filepath.Join(home, ".pi", "agent")
	}
	if c.SessionManager.AgentDir == "" {
		c.SessionManager.AgentDir = c.Pi.AgentDir
	}

	// Set defaults for database
	if c.Database.URL == "" {
		c.Database.URL = "bridge.db"
	}
	if c.Database.PoolSize == 0 {
		c.Database.PoolSize = 5
	}
	if c.Database.MaxIdle == 0 {
		c.Database.MaxIdle = 2
	}
	if c.Database.MaxLifetime == 0 {
		c.Database.MaxLifetime = 3600
	}

	// Set defaults for logging
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "console"
	}
	if len(c.Logging.Writers) == 0 {
		c.Logging.Writers = []string{"stdout"}
	}

	return nil
}

func expandEnv(s string) string {
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		return os.Getenv(s[2 : len(s)-1])
	}
	return s
}

// getHostname returns the system hostname or "unknown" on error.
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func (c *BridgeConfig) SessionTimeoutDuration() time.Duration {
	if c.SessionTimeout == 0 {
		return 0
	}
	return time.Duration(c.SessionTimeout) * time.Second
}

func (c *SessionManagerConfig) SessionTimeoutDuration() time.Duration {
	if c.SessionTimeout == 0 {
		return 0
	}
	return time.Duration(c.SessionTimeout) * time.Second
}

// Load reads and parses the configuration from a file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return Parse(data)
}

// Parse parses configuration from YAML data.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	if err := cfg.Normalize(); err != nil {
		return nil, fmt.Errorf("failed to normalize config: %w", err)
	}
	return &cfg, nil
}

// GetExampleConfig returns the example configuration.
func GetExampleConfig() string {
	return `# pi-matrix - A Matrix appservice that talks to the forge REST API.
#
# The matrix appservice no longer runs its own session manager. It
# forwards /start, /stop, room messages, and event delivery to forge
# over HTTP, and uses forge's message log as the source of truth for
# agent output.

homeserver:
    address: http://localhost:8008
    domain: localhost

appservice:
    id: pi-matrix
    localpart: pi-matrix
    url: http://localhost:29318
    registration_path: registration.yaml
    auto_generate_registration: true
    as_token: "${APPSERVICE_AS_TOKEN}"
    hs_token: "${APPSERVICE_HS_TOKEN}"

api:
    host: 0.0.0.0
    port: 8080

bridge:
    room_name_prefix: "Pi"
    auto_create_rooms: true
    delete_rooms_on_exit: false
    max_sessions: 10
    session_timeout: 0

# Forge is the durable backend that owns the agent sessions. The
# appservice talks to it over REST; forge handles spawning pi,
# running tools, and persisting the message log.
forge:
    url: http://localhost:8080
    api_key: ""

    # Reconnect backoff for the SSE stream. The consumer
    # reconnects with exponential backoff on transient errors;
    # these set the min and max of the range.
    reconnect_min_ms: 500
    reconnect_max_ms: 30000

    # Idle window after which a typing_stop is emitted. The agent
    # is assumed to be done with a turn when no events have
    # arrived on the SSE stream for this long.
    typing_quiet_ms: 3000

    # Template for new forge profiles. The matrix appservice
    # mints one profile per working directory on first /start;
    # this template is the source of provider, model, system
    # prompt, and tools.
    default_profile:
        provider: anthropic
        model: claude-sonnet-4-20250514
        base_url: ""
        api_key: ""
        system_prompt: "You are a helpful coding assistant."
        tools:
            - bash
            - read
            - write
            - edit

logging:
    level: info
    format: console
    writers:
        - stdout
`
}
