#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$PROJECT_ROOT"

KSQL_HOST_URL=${KSQL_HOST_URL:-http://localhost:8088}
KSQL_INTERNAL_URL=${KSQL_INTERNAL_URL:-http://ksqldb:8088}
SCRIPT_PATH=${1:-ksql/streams.sql}

wait_for_ksql() {
  local retries=40
  local delay=3
  echo "Waiting for ksqlDB server at ${KSQL_HOST_URL}..."
  for attempt in $(seq 1 "$retries"); do
    if curl -sSf "${KSQL_HOST_URL}/info" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay"
  done
  echo "Timed out waiting for ksqlDB at ${KSQL_HOST_URL}." >&2
  return 1
}

wait_for_ksql

echo "Applying ksqlDB topology from ${SCRIPT_PATH}..."
docker compose run --rm -T ksql-cli ksql "${KSQL_INTERNAL_URL}" -f "/scripts/$(basename "${SCRIPT_PATH}")"
