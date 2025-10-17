#!/usr/bin/env bash
set -euo pipefail

REST_PROXY_URL=${REST_PROXY_URL:-http://localhost:8082}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
TOPIC=${ORDER_TOPIC:-dom.order.placed.v1}
SUBJECT=${ORDER_SUBJECT:-dom.order.placed.v1-value}
CUSTOMER_TOPIC=${CUSTOMER_TOPIC:-dom.customer.profile.upsert.v1}
CUSTOMER_SUBJECT=${CUSTOMER_SUBJECT:-dom.customer.profile.upsert.v1-value}

echo "Fetching schema id for $SUBJECT"
schema_tmp=$(mktemp)
metadata_tmp=$(mktemp)
response_tmp=$(mktemp)
customer_response_tmp=""
cleanup() {
  rm -f "$schema_tmp" "$metadata_tmp" "$response_tmp"
  if [[ -n "$customer_response_tmp" ]]; then
    rm -f "$customer_response_tmp"
  fi
}
trap cleanup EXIT

http_code=$(curl -s -o "$schema_tmp" -w "%{http_code}" \
  "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions/latest") || http_code="000"

if [[ "$http_code" != "200" ]]; then
  echo "Unable to resolve schema id for $SUBJECT (HTTP $http_code)" >&2
  if [[ -s "$schema_tmp" ]]; then
    cat "$schema_tmp" >&2
  fi
  exit 1
fi

schema_id=$(python3 - "$schema_tmp" <<'PY'
import json, sys, pathlib
path = pathlib.Path(sys.argv[1])
with path.open() as f:
    payload = json.load(f)
schema_id = payload.get('id')
if not schema_id:
    raise SystemExit("Schema id missing from response")
print(schema_id)
PY
)

echo "Fetching schema id for $CUSTOMER_SUBJECT"
http_code=$(curl -s -o "$schema_tmp" -w "%{http_code}" \
  "$SCHEMA_REGISTRY_URL/subjects/$CUSTOMER_SUBJECT/versions/latest") || http_code="000"

if [[ "$http_code" != "200" ]]; then
  echo "Unable to resolve schema id for $CUSTOMER_SUBJECT (HTTP $http_code)" >&2
  if [[ -s "$schema_tmp" ]]; then
    cat "$schema_tmp" >&2
  fi
  exit 1
fi

customer_schema_id=$(python3 - "$schema_tmp" <<'PY'
import json, sys, pathlib
path = pathlib.Path(sys.argv[1])
with path.open() as f:
    payload = json.load(f)
schema_id = payload.get('id')
if not schema_id:
    raise SystemExit("Schema id missing from response")
print(schema_id)
PY
)

payload=$(SCHEMA_ID="$schema_id" METADATA_PATH="$metadata_tmp" FORCE_NEW_CUSTOMER="${FORCE_NEW_CUSTOMER:-}" FORCE_EXISTING_CUSTOMER="${FORCE_EXISTING_CUSTOMER:-}" python3 - <<'PY'
import json
import os
import random
import sys
import time

rng = random.SystemRandom()
schema_id = int(os.environ["SCHEMA_ID"])
metadata_path = os.environ["METADATA_PATH"]
force_new = os.environ.get("FORCE_NEW_CUSTOMER", "").lower() in {"1", "true", "yes"}
force_existing = os.environ.get("FORCE_EXISTING_CUSTOMER", "").lower() in {"1", "true", "yes"}

if force_new and force_existing:
    raise SystemExit("FORCE_NEW_CUSTOMER and FORCE_EXISTING_CUSTOMER cannot both be truthy")

order_id = f"ORD-{rng.randint(10_000, 99_999)}"
existing_customers = ["CUST-1000", "CUST-1001", "CUST-1002"]
first_names = [
    "Jordan", "Quinn", "Harper", "Avery", "Rowan", "Kai", "Emerson", "Milan"
]
last_names = [
    "Rivera", "Kim", "Singh", "Nakamura", "Diaz", "Clark", "Nguyen", "Patel"
]
email_domains = ["example.com", "shop.example.com", "mail.example.com"]
lifecycle_stages = ["prospect", "customer", "evangelist"]

if force_new:
    branch = "new"
elif force_existing:
    branch = "existing"
elif rng.random() < 0.5:
    branch = "existing"
else:
    branch = "new"

if branch == "existing":
    customer_id = rng.choice(existing_customers)
    customer_origin = "existing"
    customer_profile = None
else:
    customer_id = f"CUST-{rng.randint(10_000, 99_999)}"
    customer_origin = "new"
    first_name = rng.choice(first_names)
    last_name = rng.choice(last_names)
    email_local = f"{first_name}.{last_name}{rng.randint(100, 999)}".lower()
    customer_profile = {
        "customer_id": customer_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": f"{email_local}@{rng.choice(email_domains)}",
        "lifecycle_stage": rng.choice(lifecycle_stages),
        "is_active": rng.random() < 0.9
    }

catalog = [
    ("SKU-ALPHA", 79.99),
    ("SKU-BETA", 25.00),
    ("SKU-GAMMA", 59.50),
    ("SKU-DELTA", 30.00),
    ("SKU-OMEGA", 120.00),
    ("SKU-PHI", 19.00)
]
item_count = rng.randint(1, min(3, len(catalog)))
items = []
for sku, price in rng.sample(catalog, k=item_count):
    quantity = rng.randint(1, 3)
    items.append({
        "sku": sku,
        "quantity": quantity,
        "price": round(price, 2)
    })

order_total = round(sum(item["quantity"] * item["price"] for item in items), 2)
channel = rng.choice(["web", "store", "mobile"])
placed_at = int(time.time() * 1000)

if customer_profile:
    lookback_seconds = rng.randint(0, 30 * 24 * 60 * 60)
    created_at = placed_at - (lookback_seconds * 1000)
    customer_profile["created_at"] = created_at if created_at > 0 else placed_at

payload = {
    "key_schema": "\"string\"",
    "value_schema_id": schema_id,
    "records": [
        {
            "key": order_id,
            "value": {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_total": order_total,
                "items": items,
                "currency": "USD",
                "channel": {"string": channel},
                "placed_at": placed_at
            }
        }
    ]
}

with open(metadata_path, "w", encoding="utf-8") as fh:
    metadata = {
        "order_id": order_id,
        "customer_id": customer_id,
        "customer_origin": customer_origin,
        "order_total": order_total,
        "channel": channel,
        "placed_at": placed_at
    }
    if customer_profile:
        metadata["new_customer_profile"] = customer_profile
    json.dump(metadata, fh)

print(json.dumps(payload))
PY
)

IFS='|' read -r order_id customer_id customer_origin order_total channel placed_at < <(python3 - "$metadata_tmp" <<'PY'
import json, sys, pathlib
with pathlib.Path(sys.argv[1]).open() as fh:
    meta = json.load(fh)
print(
    "|".join(
        str(meta[field]) for field in (
            "order_id",
            "customer_id",
            "customer_origin",
            "order_total",
            "channel",
            "placed_at"
        )
    )
)
PY
)

if [[ "$customer_origin" == "new" ]]; then
  echo "Publishing new customer profile for $customer_id to $CUSTOMER_TOPIC"
  customer_payload=$(CUSTOMER_SCHEMA_ID="$customer_schema_id" METADATA_PATH="$metadata_tmp" python3 - <<'PY'
import json
import os
import pathlib

metadata_path = pathlib.Path(os.environ["METADATA_PATH"])
schema_id = int(os.environ["CUSTOMER_SCHEMA_ID"])
with metadata_path.open() as fh:
    meta = json.load(fh)
profile = meta.get("new_customer_profile")
if not profile:
    raise SystemExit("new_customer_profile missing from metadata")
payload = {
    "key_schema": "\"string\"",
    "value_schema_id": schema_id,
    "records": [
        {
            "key": meta["customer_id"],
            "value": {
                "customer_id": profile["customer_id"],
                "email": {"string": profile["email"]},
                "first_name": {"string": profile["first_name"]},
                "last_name": {"string": profile["last_name"]},
                "created_at": profile["created_at"],
                "lifecycle_stage": {"string": profile["lifecycle_stage"]},
                "is_active": bool(profile["is_active"])
            }
        }
    ]
}
print(json.dumps(payload))
PY
)

  customer_response_tmp=$(mktemp)
  customer_response_code=$(curl -s -o "$customer_response_tmp" -w "%{http_code}" \
    -X POST \
    -H "Content-Type: application/vnd.kafka.avro.v2+json" \
    --data "$customer_payload" \
    "$REST_PROXY_URL/topics/$CUSTOMER_TOPIC")

  if [[ "$customer_response_code" != "200" ]]; then
    echo "Customer seed request failed (HTTP $customer_response_code):"
    cat "$customer_response_tmp"
    exit 1
  fi

  cat "$customer_response_tmp"
  rm -f "$customer_response_tmp"
  customer_response_tmp=""

  IFS='|' read -r first_name last_name email lifecycle_stage is_active created_at < <(python3 - "$metadata_tmp" <<'PY'
import json, pathlib, sys
with pathlib.Path(sys.argv[1]).open() as fh:
    meta = json.load(fh)
profile = meta.get("new_customer_profile", {})
fields = [
    profile.get("first_name", ""),
    profile.get("last_name", ""),
    profile.get("email", ""),
    profile.get("lifecycle_stage", ""),
    profile.get("is_active", ""),
    profile.get("created_at", "")
]
print("|".join(str(field) for field in fields))
PY
  )

  echo "Added customer profile $customer_id ($first_name $last_name, $email) lifecycle_stage=$lifecycle_stage active=$is_active created_at_ms=$created_at"
fi

echo "Seeding $TOPIC via $REST_PROXY_URL"
response_code=$(curl -s -o "$response_tmp" -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  --data "$payload" \
  "$REST_PROXY_URL/topics/$TOPIC")

if [[ "$response_code" != "200" ]]; then
  echo "Seed request failed (HTTP $response_code):"
  cat "$response_tmp"
  exit 1
fi

cat "$response_tmp"

echo "Random order $order_id for $customer_origin customer $customer_id published to $TOPIC"
echo "Details: total=$order_total channel=$channel placed_at_ms=$placed_at"
