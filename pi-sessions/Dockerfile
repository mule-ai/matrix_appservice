# pi-matrix - A Matrix appservice for pi sessions via RPC mode
# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-linkmode external -extldflags '-static'" -o pi-matrix ./cmd/pi-matrix

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install ca-certificates for HTTPS
RUN apk add --no-cache ca-certificates

# Copy binary from builder
COPY --from=builder /app/pi-matrix .

# Copy config if exists
COPY config.yaml.example /app/config.yaml

# Create non-root user
RUN adduser -D -g '' appuser
USER appuser

# Expose ports
EXPOSE 8080 29318

# Run
CMD ["./pi-matrix", "-c", "/app/config.yaml"]
