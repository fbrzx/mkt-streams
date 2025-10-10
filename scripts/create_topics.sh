#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$PROJECT_ROOT"

REQUIRED_TOPICS=(
  "dom.customer.profile.upsert.v1"
  "dom.order.placed.v1"
  "dom.activation.delivery.status.v1"
  "dom.customer.order.enriched.v1"
  "dom.segment.source.v1"
  "dom.segment.materialized.v1"
)

wait_for_redpanda() {
  local retries=30
  local delay=2
  echo "Waiting for Redpanda to become available..."
  for attempt in $(seq 1 "$retries"); do
    if docker compose exec -T redpanda rpk cluster metadata >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay"
  done
  echo "Redpanda cluster is not reachable after $((retries * delay)) seconds." >&2
  return 1
}

create_topic() {
  local topic=$1
  if docker compose exec -T redpanda rpk topic describe "$topic" >/dev/null 2>&1; then
    echo "Topic $topic already exists."
    return 0
  fi
  echo "Creating topic $topic..."
  docker compose exec -T redpanda rpk topic create "$topic" --partitions 1 --replicas 1 >/dev/null
}

wait_for_redpanda

for topic in "${REQUIRED_TOPICS[@]}"; do
  create_topic "$topic"
done

echo "All required topics are present."
