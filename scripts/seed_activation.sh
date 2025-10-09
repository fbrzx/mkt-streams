#!/usr/bin/env bash
set -euo pipefail

REST_PROXY_URL=${REST_PROXY_URL:-http://localhost:8082}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
TOPIC=${ACTIVATION_TOPIC:-dom.activation.delivery.status.v1}
SUBJECT=${ACTIVATION_SUBJECT:-dom.activation.delivery.status.v1-value}

echo "Fetching schema id for $SUBJECT"
schema_id=$(curl -s "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions/latest" | python3 - <<'PY'
import json, sys
payload = json.load(sys.stdin)
print(payload['id'])
PY
)

if [[ -z "$schema_id" ]]; then
  echo "Unable to resolve schema id for $SUBJECT" >&2
  exit 1
fi

read -r -d '' payload <<'JSON'
{
  "key_schema": "\"string\"",
  "value_schema_id": SCHEMA_ID,
  "records": [
    {
      "key": "ACT-5000",
      "value": {
        "activation_id": "ACT-5000",
        "order_id": "ORD-9000",
        "customer_id": "CUST-1000",
        "channel": "email",
        "status": "DELIVERED",
        "delivered_at": 1704509200000,
        "attempts": 1
      }
    },
    {
      "key": "ACT-5001",
      "value": {
        "activation_id": "ACT-5001",
        "order_id": "ORD-9001",
        "customer_id": "CUST-1001",
        "channel": "sms",
        "status": "BOUNCED",
        "delivered_at": null,
        "attempts": 3
      }
    }
  ]
}
JSON

payload=${payload//SCHEMA_ID/$schema_id}

echo "Seeding $TOPIC via $REST_PROXY_URL"
response=$(curl -s -o /tmp/activation_seed_response.json -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  --data "$payload" \
  "$REST_PROXY_URL/topics/$TOPIC")

if [[ "$response" != "200" ]]; then
  echo "Seed request failed (HTTP $response):"
  cat /tmp/activation_seed_response.json
  exit 1
fi

cat /tmp/activation_seed_response.json
rm -f /tmp/activation_seed_response.json

echo "Activation status records published to $TOPIC"
