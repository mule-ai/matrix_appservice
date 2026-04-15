#!/bin/bash
# Run pi-matrix in Docker

set -e

cd "$(dirname "$0")"

# Default values
IMAGE_NAME="pi-matrix"
CONTAINER_NAME="pi-matrix"
CONFIG_FILE="${PI_MATRIX_CONFIG:-./config.yaml}"

# Build image if needed
if ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
    echo "Building Docker image..."
    docker build -t "$IMAGE_NAME" .
fi

# Stop existing container
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Stopping existing container..."
    docker stop "$CONTAINER_NAME" 2>/dev/null || true
    docker rm "$CONTAINER_NAME" 2>/dev/null || true
fi

# Run container
echo "Starting pi-matrix..."
docker run \
    --name "$CONTAINER_NAME" \
    --env-file <(env | grep -E '^(APPSERVICE_|PI_)') \
    -v "$CONFIG_FILE:/app/config.yaml:ro" \
    -p 8080:8080 \
    -p 29318:29318 \
    "$IMAGE_NAME"
