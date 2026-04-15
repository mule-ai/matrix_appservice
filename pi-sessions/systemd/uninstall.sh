#!/bin/bash
# Uninstall pi-matrix services
set -e

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root (use sudo)"
    exit 1
fi

echo "Uninstalling pi-matrix services..."

# Stop services
echo "Stopping services..."
systemctl stop pi-matrix 2>/dev/null || true
systemctl stop pi-session-manager 2>/dev/null || true

# Disable services
echo "Disabling services..."
systemctl disable pi-matrix 2>/dev/null || true
systemctl disable pi-session-manager 2>/dev/null || true

# Reload systemd
systemctl daemon-reload

# Remove systemd units
echo "Removing systemd units..."
rm -f /etc/systemd/system/pi-matrix.service
rm -f /etc/systemd/system/pi-session-manager.service

# Ask about data removal
echo ""
read -p "Remove configuration and data directories? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf /opt/pi-matrix
    rm -rf /etc/pi-matrix
    rm -rf /var/log/pi-matrix
    rm -rf /var/lib/pi
    echo "Removed data directories."
else
    echo "Kept data directories."
fi

echo "Uninstallation complete."
