#!/bin/sh
set -eu

# If the user supplied a command (docker-compose "command:"), run it.
# This makes the container behave normally and avoids "everything becomes router".
if [ "$#" -gt 0 ]; then
  exec "$@"
fi

ROLE="${ROLE:-router}"
PORT="${PORT:-8000}"
HOST="${HOST:-0.0.0.0}"

# StatefulSets: hostname ends with -<ordinal> (e.g., shard0-2 => 2)
ORDINAL="${HOSTNAME##*-}"

case "$ROLE" in
  router)
    exec python -m uvicorn app.router_main:app --host "$HOST" --port "$PORT"
    ;;
  coordinator)
    exec python -m uvicorn app.coordinator_main:app --host "$HOST" --port "$PORT"
    ;;
  shard)
    export REPLICA_ID="${REPLICA_ID:-$ORDINAL}"
    exec python -m uvicorn app.shard_main:app --host "$HOST" --port "$PORT"
    ;;
  *)
    echo "Unknown ROLE: $ROLE (expected: router, coordinator, shard)" >&2
    exit 1
    ;;
esac