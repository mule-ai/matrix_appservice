#!/bin/bash
# Build script for pi-matrix

set -e

cd "$(dirname "$0")"

echo "Building pi-matrix..."

# Determine Go version
GO_VERSION=$(go version | grep -oP 'go\K[0-9]+\.[0-9]+')
echo "Using Go $GO_VERSION"

# Build for current platform
echo "Building for $(uname -s)/$(uname -m)..."
GOOS=$(uname -s | tr '[:upper:]' '[:lower:]')
GOARCH=$(uname -m)

# Map architecture names
case "$(uname -m)" in
    x86_64) GOARCH="amd64" ;;
    aarch64|arm64) GOARCH="arm64" ;;
esac

OUTPUT="pi-matrix-${GOOS}-${GOARCH}"

CGO_ENABLED=0 go build -ldflags "-X main.Tag=$(git describe --tags 2>/dev/null || echo 'dev') -X main.Commit=$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown') -X main.BuildTime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" -o "$OUTPUT" ./cmd/pi-matrix

echo "Build complete: $OUTPUT"
echo ""
echo "Run with: ./$OUTPUT -c config.yaml"
