# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download

# Copy source
COPY . .

# Build binary with optimizations
RUN GOTOOLCHAIN=auto CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o /zapfs \
    .

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM alpine:3.21

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 zapfs && \
    adduser -u 1000 -G zapfs -s /bin/sh -D zapfs

# Create data directories
RUN mkdir -p /data/chunks /data/metadata /data/raft && \
    chown -R zapfs:zapfs /data

# Copy binary and configs
COPY --from=builder /zapfs /usr/local/bin/zapfs
COPY cmd/config/*.toml /etc/zapfs/

# Set ownership
RUN chown -R zapfs:zapfs /etc/zapfs

# Switch to non-root user
USER zapfs

# Default volumes
VOLUME ["/data"]

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:${HTTP_PORT:-8080}/health || exit 1

# Default environment
ENV HTTP_PORT=8080 \
    GRPC_PORT=8081 \
    DATA_DIR=/data

# Default command (can be overridden)
ENTRYPOINT ["zapfs"]
CMD ["--help"]
