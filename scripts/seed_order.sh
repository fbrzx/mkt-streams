#!/usr/bin/env bash
set -euo pipefail

REST_PROXY_URL=${REST_PROXY_URL:-http://localhost:8082}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
TOPIC=${ORDER_TOPIC:-dom.order.placed.v1}
SUBJECT=${ORDER_SUBJECT:-dom.order.placed.v1-value}

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
      "key": "ORD-9000",
      "value": {
        "order_id": "ORD-9000",
        "customer_id": "CUST-1000",
        "order_total": 129.99,
        "items": [
          {"sku": "SKU-ALPHA", "quantity": 1, "price": 79.99},
          {"sku": "SKU-BETA", "quantity": 2, "price": 25.00}
        ],
        "currency": "USD",
        "channel": "web",
        "placed_at": 1704326400000
      }
    },
    {
      "key": "ORD-9001",
      "value": {
        "order_id": "ORD-9001",
        "customer_id": "CUST-1001",
        "order_total": 89.50,
        "items": [
          {"sku": "SKU-GAMMA", "quantity": 1, "price": 59.50},
          {"sku": "SKU-DELTA", "quantity": 1, "price": 30.00}
        ],
        "currency": "USD",
        "channel": "store",
        "placed_at": 1704412800000
      }
    }
  ]
}
JSON

payload=${payload//SCHEMA_ID/$schema_id}

echo "Seeding $TOPIC via $REST_PROXY_URL"
response=$(curl -s -o /tmp/order_seed_response.json -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  --data "$payload" \
  "$REST_PROXY_URL/topics/$TOPIC")

if [[ "$response" != "200" ]]; then
  echo "Seed request failed (HTTP $response):"
  cat /tmp/order_seed_response.json
  exit 1
fi

cat /tmp/order_seed_response.json
rm -f /tmp/order_seed_response.json

echo "Order records published to $TOPIC"
