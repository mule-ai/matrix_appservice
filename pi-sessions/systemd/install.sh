#!/bin/bash
# Install pi-matrix services
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

echo "Installing pi-matrix services..."

# Create user if not exists
if ! id "$USER" &>/dev/null; then
    echo "Creating user '$USER'..."
    useradd -r -s /bin/false -d /var/lib/pi -c "Pi Matrix Service" "$USER"
fi

# Create directories
echo "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$CONFIG_DIR"
mkdir -p /var/log/pi-matrix
mkdir -p /var/lib/pi/agent

# Set ownership
chown -R "$USER:$GROUP" "$INSTALL_DIR"
chown -R "$USER:$GROUP" "$CONFIG_DIR"
chown -R "$USER:$GROUP" /var/log/pi-matrix
chown -R "$USER:$GROUP" /var/lib/pi

# Generate tokens
echo "Generating API tokens..."
AS_TOKEN=$(openssl rand -base64 32)
HS_TOKEN=$(openssl rand -base64 32)

# Create config files
echo "Creating configuration files..."

# Session manager config
cat > "$CONFIG_DIR/session-manager.yaml" << EOF
session_manager:
  host: 0.0.0.0
  port: 8081
  api_key: "$AS_TOKEN"
  pi_path: /usr/local/bin/pi
  agent_dir: /var/lib/pi/agent
  max_sessions: 10
  session_timeout: 3600

logging:
  level: info
  format: console
  writers:
    - stdout
EOF

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
  room_name_prefix: "Pi Session"
  auto_create_rooms: true
  delete_rooms_on_exit: false
  max_sessions: 10
  session_timeout: 3600

session_manager:
  url: http://localhost:8081
  api_key: "$AS_TOKEN"

logging:
  level: info
  format: console
  writers:
    - stdout
EOF

# Set config permissions
chmod 600 "$CONFIG_DIR"/*.yaml

# Install binaries
echo "Copying binaries..."
cp pi-session-manager "$INSTALL_DIR/"
cp pi-matrix "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR"/*

# Install systemd units
echo "Installing systemd units..."
cp systemd/pi-session-manager.service /etc/systemd/system/
cp systemd/pi-matrix.service /etc/systemd/system/

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

# Enable services
echo "Enabling services..."
systemctl enable pi-session-manager
systemctl enable pi-matrix

echo ""
echo "Installation complete!"
echo ""
echo "Token information:"
echo "  AS_TOKEN: $AS_TOKEN"
echo "  HS_TOKEN: $HS_TOKEN"
echo ""
echo "These tokens are saved in: $CONFIG_DIR/config.yaml"
echo ""
echo "To start services:"
echo "  sudo systemctl start pi-session-manager"
echo "  sudo systemctl start pi-matrix"
echo ""
echo "To check status:"
echo "  sudo systemctl status pi-session-manager"
echo "  sudo systemctl status pi-matrix"
echo ""
echo "To view logs:"
echo "  journalctl -u pi-session-manager -f"
echo "  journalctl -u pi-matrix -f"
