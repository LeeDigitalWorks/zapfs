#!/bin/sh
# Development entrypoint for hot reload
#
# Usage: dev-entrypoint.sh <service> [args...]
# Examples:
#   dev-entrypoint.sh manager --ip=0.0.0.0 --grpc_port=8050
#   dev-entrypoint.sh metadata --http_port=8082
#   dev-entrypoint.sh file --bind_addr=0.0.0.0:8081

set -e

SERVICE="$1"
shift

# Store args for the binary in an env var that Air can use
export ZAPFS_ARGS="$SERVICE $*"

# Select the appropriate Air config
case "$SERVICE" in
    manager)
        CONFIG="/app/.air.manager.toml"
        ;;
    metadata)
        CONFIG="/app/.air.metadata.toml"
        ;;
    file)
        CONFIG="/app/.air.file.toml"
        ;;
    *)
        echo "Unknown service: $SERVICE"
        echo "Usage: dev-entrypoint.sh <manager|metadata|file> [args...]"
        exit 1
        ;;
esac

echo "==> Starting $SERVICE with hot reload"
echo "==> Args: $ZAPFS_ARGS"
echo "==> Config: $CONFIG"

# Run Air with the config
exec air -c "$CONFIG"
