#!/bin/bash
# Install the pi-matrix appservice.
#
# The appservice is a single binary that talks to the local forge
# instance over HTTP. This script drops the binary and its config
# in /opt/pi-matrix and /etc/pi-matrix and registers a systemd
# unit.
set -e

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root (use sudo)"
    exit 1
fi

# Configuration
INSTALL_DIR="/opt/pi-matrix"
CONFIG_DIR="/etc/pi-matrix"
USER="pi"
GROUP="pi"

echo "Installing pi-matrix appservice..."

# Create user if not exists
if ! id "$USER" &>/dev/null; then
    echo "Creating user '$USER'..."
    useradd -r -s /bin/false -d /var/lib/pi-matrix -c "Pi Matrix Service" "$USER"
fi

# Create directories
echo "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$CONFIG_DIR"
mkdir -p /var/log/pi-matrix
mkdir -p /var/lib/pi-matrix

# Set ownership
chown -R "$USER:$GROUP" "$INSTALL_DIR"
chown -R "$USER:$GROUP" "$CONFIG_DIR"
chown -R "$USER:$GROUP" /var/log/pi-matrix
chown -R "$USER:$GROUP" /var/lib/pi-matrix

# Generate tokens
echo "Generating appservice tokens..."
AS_TOKEN=$(openssl rand -base64 32)
HS_TOKEN=$(openssl rand -base64 32)

# Appservice config
cat > "$CONFIG_DIR/config.yaml" << EOF
homeserver:
  address: http://localhost:8008
  domain: localhost

appservice:
  id: pi-matrix
  localpart: pi-matrix
  url: http://localhost:8080
  registration_path: $CONFIG_DIR/registration.yaml
  auto_generate_registration: true
  as_token: "$AS_TOKEN"
  hs_token: "$HS_TOKEN"

api:
  host: 0.0.0.0
  port: 8080

bridge:
  room_name_prefix: "Pi"
  auto_create_rooms: true
  delete_rooms_on_exit: false
  max_sessions: 10
  session_timeout: 0

forge:
  url: http://localhost:8080
  api_key: ""

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
EOF

# Set config permissions
chmod 600 "$CONFIG_DIR"/*.yaml

# Install binary
echo "Copying pi-matrix binary..."
cp pi-matrix "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR/pi-matrix"

# Install systemd unit
echo "Installing systemd unit..."
cp systemd/pi-matrix.service /etc/systemd/system/

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

# Enable the service
echo "Enabling pi-matrix..."
systemctl enable pi-matrix

echo ""
echo "Installation complete!"
echo ""
echo "Tokens (saved in $CONFIG_DIR/config.yaml):"
echo "  AS_TOKEN: $AS_TOKEN"
echo "  HS_TOKEN: $HS_TOKEN"
echo ""
echo "Before starting, edit $CONFIG_DIR/config.yaml and set forge.api_key"
echo "to the same value as one of forge's API keys (forge's /auth/login"
echo "endpoint mints one)."
echo ""
echo "To start the service:"
echo "  sudo systemctl start pi-matrix"
echo ""
echo "To check status / logs:"
echo "  sudo systemctl status pi-matrix"
echo "  journalctl -u pi-matrix -f"
