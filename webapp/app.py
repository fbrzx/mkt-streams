#!/usr/bin/env python3
import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from flask import Flask, render_template, request
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from redis import Redis
from redis.exceptions import RedisError


app = Flask(__name__)

DEFAULT_KSQLDB_URL = "http://localhost:8088"
DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081"
DEFAULT_REST_PROXY_URL = "http://localhost:8082"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_CONSOLIDATED_TOPIC = "dom.customer.consolidated.v1"

ORDER_TOPIC = os.environ.get("ORDER_TOPIC", "dom.order.placed.v1")
ORDER_SUBJECT = os.environ.get("ORDER_SUBJECT", f"{ORDER_TOPIC}-value")
CUSTOMER_TOPIC = os.environ.get("CUSTOMER_TOPIC", "dom.customer.profile.upsert.v1")
CUSTOMER_SUBJECT = os.environ.get("CUSTOMER_SUBJECT", f"{CUSTOMER_TOPIC}-value")

RESULT_LIMIT = int(os.environ.get("CUSTOMER_RESULT_LIMIT", "250"))

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
CONSOLIDATED_TOPIC = os.environ.get("CONSOLIDATED_TOPIC", DEFAULT_CONSOLIDATED_TOPIC)

CONSOLIDATED_COLUMNS = (
    "customer_id",
    "order_count",
    "lifetime_value",
    "last_order_ts",
    "first_name",
    "last_name",
    "email",
    "lifecycle_stage",
    "is_active",
)

AVRO_HEADERS = {"Content-Type": "application/vnd.kafka.avro.v2+json"}

_schema_id_cache: Dict[str, int] = {}
_http = requests.Session()
_rng = random.SystemRandom()
_redis_client: Optional[Redis] = None
_redis_lock = threading.Lock()
_cache_sync_started = False

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))


class EventPublishError(Exception):
    """Raised when publishing to Kafka via REST Proxy fails."""


class DataUnavailableError(Exception):
    """Raised when the consolidated snapshot cannot be retrieved."""


@dataclass
class OrderEvent:
    order_id: str
    customer_id: str
    order_total: float
    items: List[Dict[str, object]]
    channel: str
    placed_at: int

    def value(self) -> Dict[str, object]:
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "order_total": self.order_total,
            "items": self.items,
            "currency": "USD",
            "channel": {"string": self.channel},
            "placed_at": self.placed_at,
        }


@dataclass
class CustomerProfile:
    customer_id: str
    email: str
    first_name: str
    last_name: str
    lifecycle_stage: str
    is_active: bool
    created_at: int

    def value(self) -> Dict[str, object]:
        def nullable_string(value: Optional[str]) -> Optional[Dict[str, str]]:
            return {"string": value} if value is not None else None

        return {
            "customer_id": self.customer_id,
            "email": nullable_string(self.email),
            "first_name": nullable_string(self.first_name),
            "last_name": nullable_string(self.last_name),
            "created_at": self.created_at,
            "lifecycle_stage": nullable_string(self.lifecycle_stage),
            "is_active": self.is_active,
        }


def schema_registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL).rstrip("/")


def rest_proxy_url() -> str:
    return os.environ.get("REST_PROXY_URL", DEFAULT_REST_PROXY_URL).rstrip("/")


def ksqldb_url() -> str:
    return os.environ.get("KSQLDB_URL", DEFAULT_KSQLDB_URL).rstrip("/")


def redis_url() -> str:
    return os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)


def get_redis_client() -> Optional[Redis]:
    global _redis_client
    if _redis_client:
        return _redis_client

    with _redis_lock:
        if _redis_client:
            return _redis_client
        url = redis_url()
        try:
            client = Redis.from_url(url, decode_responses=True)
            # Probe the connection so we fail fast if Redis is unreachable.
            client.ping()
        except RedisError as exc:
            logger.warning("Redis unavailable at %s (%s)", url, exc)
            return None
        _redis_client = client
        logger.info("Connected to Redis at %s", url)
        return _redis_client


def normalise_row(raw: Dict, fallback_id: Optional[str] = None) -> Dict:
    if not isinstance(raw, dict):
        return {}

    row: Dict[str, Optional[object]] = {}
    for key, value in raw.items():
        row[str(key).lower()] = value

    if fallback_id and not row.get("customer_id"):
        row["customer_id"] = fallback_id

    customer_id = row.get("customer_id")
    if customer_id is not None:
        row["customer_id"] = str(customer_id)

    def extract_string(value):
        current = value
        visited = 0
        while isinstance(current, dict) and visited < 4:
            visited += 1
            if "string" in current:
                current = current.get("string")
                continue
            if "STRING" in current:
                current = current.get("STRING")
                continue
            if len(current) == 1:
                current = next(iter(current.values()))
                continue
            break
        return current

    for field in ("first_name", "last_name", "email", "lifecycle_stage"):
        extracted = extract_string(row.get(field))
        if isinstance(extracted, str):
            row[field] = extracted or None
        elif extracted is None:
            row[field] = None
        else:
            row[field] = extracted

    order_count = row.get("order_count")
    try:
        row["order_count"] = int(order_count) if order_count is not None else 0
    except (TypeError, ValueError):
        row["order_count"] = 0

    lifetime_value = row.get("lifetime_value")
    try:
        row["lifetime_value"] = float(lifetime_value) if lifetime_value is not None else 0.0
    except (TypeError, ValueError):
        row["lifetime_value"] = 0.0

    last_order_ts = row.get("last_order_ts")
    try:
        parsed_ts = int(last_order_ts) if last_order_ts is not None else None
    except (TypeError, ValueError):
        parsed_ts = None
    row["last_order_ts"] = parsed_ts
    if isinstance(parsed_ts, (int, float)):
        row["last_order_at"] = datetime.fromtimestamp(parsed_ts / 1000.0, tz=timezone.utc).isoformat()
    else:
        row["last_order_at"] = None

    is_active = row.get("is_active")
    if isinstance(is_active, str):
        lowered = is_active.lower()
        if lowered in {"true", "1", "yes", "y"}:
            row["is_active"] = True
        elif lowered in {"false", "0", "no", "n"}:
            row["is_active"] = False
        else:
            row["is_active"] = None
    elif isinstance(is_active, bool) or is_active is None:
        row["is_active"] = is_active
    else:
        row["is_active"] = bool(is_active)

    return row


def row_from_columns(columns: List, fallback_id: Optional[str] = None) -> Dict:
    raw = {name: columns[idx] if idx < len(columns) else None for idx, name in enumerate(CONSOLIDATED_COLUMNS)}
    return normalise_row(raw, fallback_id)


def enrich_row_with_profile(row: Dict, attempts: int = 1, delay: float = 0.1) -> Dict:
    customer_id = row.get("customer_id")
    if not customer_id:
        return row
    if row.get("first_name") or row.get("email"):
        return row
    profile = wait_for_customer_profile(customer_id, attempts=attempts, delay=delay)
    if profile:
        row.update({k: v for k, v in profile.items() if v is not None})
        cache_row(row)
    return row


def cache_row(row: Dict) -> None:
    client = get_redis_client()
    if not client:
        return
    try:
        normalised = normalise_row(row)
        customer_id = normalised.get("customer_id")
        if not customer_id:
            return
        score = normalised.get("last_order_ts") or 0
        client.zadd("customers", {customer_id: score})
        client.set(f"customer:{customer_id}", json.dumps(normalised))
    except RedisError as exc:
        logger.warning("Unable to cache row for %s: %s", row.get("customer_id"), exc)


def remove_cached_row(customer_id: str) -> None:
    client = get_redis_client()
    if not client:
        return
    try:
        client.zrem("customers", customer_id)
        client.delete(f"customer:{customer_id}")
    except RedisError as exc:
        logger.warning("Unable to delete cached row for %s: %s", customer_id, exc)


def reset_cache(prefix: str = "customer:") -> None:
    client = get_redis_client()
    if not client:
        return
    try:
        pipe = client.pipeline(transaction=False)
        pipe.delete("customers")
        cursor = 0
        pattern = f"{prefix}*"
        while True:
            cursor, keys = client.scan(cursor=cursor, match=pattern, count=500)
            if keys:
                for key in keys:
                    pipe.delete(key)
            if cursor == 0:
                break
        pipe.execute()
        logger.info("Cleared cached customer view in Redis (pattern %s)", pattern)
    except RedisError as exc:
        logger.warning("Unable to clear cached state from Redis: %s", exc)


def upsert_cached_customer(row: Dict) -> None:
    customer_id = row.get("customer_id")
    if not customer_id:
        return
    current = load_cached_customer(customer_id)
    incoming = normalise_row(row, customer_id)
    if not (incoming.get("first_name") or incoming.get("email")):
        profile_snapshot = wait_for_customer_profile(customer_id, attempts=3, delay=0.3)
        if profile_snapshot:
            incoming.update({k: v for k, v in profile_snapshot.items() if v is not None})
    merged: Dict[str, object] = dict(current) if current else {}
    for key, value in incoming.items():
        if value is not None or key not in merged:
            merged[key] = value
    cache_row(merged)


def load_cached_customer(customer_id: Optional[str]) -> Dict:
    if not customer_id:
        return {}
    client = get_redis_client()
    if not client:
        return {}
    try:
        payload = client.get(f"customer:{customer_id}")
        if not payload:
            return {}
        raw = json.loads(payload)
        row = normalise_row(raw, customer_id)
        return row
    except (RedisError, json.JSONDecodeError) as exc:
        logger.warning("Unable to load cached customer %s: %s", customer_id, exc)
        return {}


def load_cached_rows(limit: int) -> List[Dict]:
    client = get_redis_client()
    if not client:
        return []
    try:
        ids = client.zrevrange("customers", 0, max(limit - 1, 0))
        rows: List[Dict] = []
        for customer_id in ids:
            row = load_cached_customer(customer_id)
            if row:
                rows.append(enrich_row_with_profile(row, attempts=2, delay=0.2))
        rows.sort(key=lambda record: record.get("last_order_ts") or 0, reverse=True)
        return rows
    except RedisError as exc:
        logger.warning("Unable to read cached rows: %s", exc)
        return []


def start_cache_sync_thread() -> None:
    global _cache_sync_started
    if _cache_sync_started:
        return

    def worker():
        backoff = 2
        max_backoff = 30
        bootstrap = [server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",") if server.strip()]

        while True:
            client = get_redis_client()
            if not client:
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue

            consumer: Optional[KafkaConsumer] = None
            try:
                consumer = KafkaConsumer(
                    CONSOLIDATED_TOPIC,
                    bootstrap_servers=bootstrap,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    group_id="webapp-cache-sync",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
                    key_deserializer=lambda m: m.decode("utf-8") if m else None,
                    consumer_timeout_ms=10000,
                )
                consumer.poll(timeout_ms=0)
                assignment = consumer.assignment()
                if assignment:
                    for tp in assignment:
                        consumer.seek_to_beginning(tp)
                backoff = 2
                logger.info("Cache sync consuming %s via %s", CONSOLIDATED_TOPIC, bootstrap)

                while True:
                    try:
                        record = next(consumer)
                    except StopIteration:
                        continue

                    key = record.key
                    value = record.value
                    logger.info("Kafka record key=%s value=%s", key, value)

                    if value is None:
                        if key:
                            remove_cached_row(key)
                        continue

                    if not isinstance(value, dict):
                        logger.debug("Skipping non-dict value for %s: %s", key, value)
                        continue

                    row = normalise_row(value, key)
                    logger.info("Cache update from topic (normalised): %s", row)
                    if not row:
                        continue

                    upsert_cached_customer(row)
            except (KafkaError, ValueError, UnicodeDecodeError) as exc:
                logger.warning("Cache sync consumer error (%s). Retrying in %s seconds.", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

    thread = threading.Thread(target=worker, name="CacheSyncThread", daemon=True)
    thread.start()
    _cache_sync_started = True
    logger.info("Started cache sync thread.")


def query_customer_snapshot(customer_id: str, timeout: float = 5.0) -> Optional[Dict]:
    sanitized_id = customer_id.replace("'", "''")
    statement = (
        "SELECT customer_id, order_count, lifetime_value, last_order_ts, first_name, last_name, email, "
        "lifecycle_stage, is_active "
        f"FROM T_CUSTOMER_CONSOLIDATED WHERE customer_id = '{sanitized_id}';"
    )
    payload = {
        "ksql": statement,
        "streamsProperties": {},
    }

    try:
        response = _http.post(
            f"{ksqldb_url()}/query",
            data=json.dumps(payload),
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Pull query for %s failed: %s", customer_id, exc)
        return None

    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        try:
            message = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if isinstance(message, list):
            for entry in message:
                if not isinstance(entry, dict):
                    continue
                row = entry.get("row")
                if not row:
                    continue
                columns = row.get("columns") or []
                if not columns:
                    continue
                return row_from_columns(columns, customer_id)
            continue
        row = message.get("row")
        if not row:
            continue
        columns = row.get("columns") or []
        if not columns:
            continue
        return row_from_columns(columns, customer_id)
    return None


def prime_customer_snapshot(customer_id: str, attempts: int = 6, delay: float = 0.5) -> None:
    for _ in range(max(attempts, 1)):
        snapshot = query_customer_snapshot(customer_id)
        if snapshot:
            cache_row(snapshot)
            normalized = normalise_row(snapshot, customer_id)
            has_profile = normalized.get("first_name") or normalized.get("email") or normalized.get("last_name")
            if has_profile:
                return
        time.sleep(delay)


def query_customer_profile(customer_id: str, timeout: float = 5.0) -> Optional[Dict]:
    sanitized_id = customer_id.replace("'", "''")
    statement = (
        "SELECT customer_id, first_name, last_name, email, lifecycle_stage, is_active "
        f"FROM T_CUSTOMER_PROFILE WHERE customer_id = '{sanitized_id}';"
    )
    payload = {
        "ksql": statement,
        "streamsProperties": {},
    }

    try:
        response = _http.post(
            f"{ksqldb_url()}/query",
            data=json.dumps(payload),
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Profile pull query for %s failed: %s", customer_id, exc)
        return None

    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        try:
            message = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        payloads = message if isinstance(message, list) else [message]
        for entry in payloads:
            if not isinstance(entry, dict):
                continue
            row = entry.get("row")
            if not row:
                continue
            columns = row.get("columns") or []
            if len(columns) < 6:
                continue
            return {
                "customer_id": columns[0],
                "first_name": columns[1],
                "last_name": columns[2],
                "email": columns[3],
                "lifecycle_stage": columns[4],
                "is_active": columns[5],
            }
    return None


def wait_for_customer_profile(customer_id: str, attempts: int = 6, delay: float = 0.5) -> Optional[Dict]:
    for _ in range(max(attempts, 1)):
        profile = query_customer_profile(customer_id)
        if profile and (profile.get("first_name") or profile.get("email")):
            return profile
        time.sleep(delay)
    return None
def get_schema_id(subject: str) -> int:
    if subject in _schema_id_cache:
        return _schema_id_cache[subject]

    url = f"{schema_registry_url()}/subjects/{subject}/versions/latest"
    try:
        response = _http.get(url, timeout=5)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise EventPublishError(f"Unable to fetch schema id for {subject}: {exc}") from exc

    payload = response.json()
    schema_id = payload.get("id")
    if not isinstance(schema_id, int):
        raise EventPublishError(f"Schema Registry response missing id for {subject}")
    _schema_id_cache[subject] = schema_id
    return schema_id


def post_avro(topic: str, payload: Dict[str, object]) -> None:
    url = f"{rest_proxy_url()}/topics/{topic}"
    try:
        response = _http.post(url, headers=AVRO_HEADERS, data=json.dumps(payload), timeout=10)
    except requests.RequestException as exc:
        raise EventPublishError(f"Unable to reach REST Proxy at {url}: {exc}") from exc

    if response.status_code != 200:
        details = response.text.strip() or response.reason
        raise EventPublishError(f"REST Proxy returned HTTP {response.status_code}: {details}")


def random_catalog_entry() -> Tuple[str, float]:
    catalog = [
        ("SKU-ALPHA", 79.99),
        ("SKU-BETA", 25.00),
        ("SKU-GAMMA", 45.75),
        ("SKU-OMEGA", 120.00),
        ("SKU-SIGMA", 30.12),
        ("SKU-DELTA", 58.40),
    ]
    return _rng.choice(catalog)


def build_random_order(customer_id: str) -> OrderEvent:
    order_id = f"ORD-{_rng.randint(10_000, 99_999)}"
    item_count = _rng.randint(1, 3)
    items: List[Dict[str, object]] = []
    order_total = 0.0
    for _ in range(item_count):
        sku, price = random_catalog_entry()
        quantity = _rng.randint(1, 3)
        items.append(
            {
                "sku": sku,
                "quantity": quantity,
                "price": round(price, 2),
            }
        )
        order_total += price * quantity

    channel = _rng.choice(["web", "mobile", "store", "call_center"])
    placed_at = int(time.time() * 1000)
    return OrderEvent(
        order_id=order_id,
        customer_id=customer_id,
        order_total=round(order_total, 2),
        items=items,
        channel=channel,
        placed_at=placed_at,
    )


def build_random_customer() -> CustomerProfile:
    first_names = ["Jordan", "Quinn", "Harper", "Avery", "Rowan", "Kai", "Emerson", "Milan"]
    last_names = ["Rivera", "Kim", "Singh", "Nakamura", "Diaz", "Clark", "Nguyen", "Patel"]
    email_domains = ["example.com", "shop.example.com", "mail.example.com"]
    lifecycle_stages = ["prospect", "customer", "evangelist", "churn-risk"]

    first_name = _rng.choice(first_names)
    last_name = _rng.choice(last_names)
    email = f"{first_name}.{last_name}{_rng.randint(100, 999)}@{_rng.choice(email_domains)}".lower()
    created_at = int(time.time() * 1000)

    return CustomerProfile(
        customer_id=f"CUST-{_rng.randint(10_000, 99_999)}",
        email=email,
        first_name=first_name,
        last_name=last_name,
        lifecycle_stage=_rng.choice(lifecycle_stages),
        is_active=_rng.random() < 0.92,
        created_at=created_at,
    )


def publish_customer_profile(profile: CustomerProfile) -> None:
    payload = {
        "key_schema": "\"string\"",
        "value_schema_id": get_schema_id(CUSTOMER_SUBJECT),
        "records": [
            {
                "key": profile.customer_id,
                "value": profile.value(),
            }
        ],
    }
    post_avro(CUSTOMER_TOPIC, payload)


def publish_order(order: OrderEvent) -> None:
    payload = {
        "key_schema": "\"string\"",
        "value_schema_id": get_schema_id(ORDER_SUBJECT),
        "records": [
            {
                "key": order.order_id,
                "value": order.value(),
            }
        ],
    }
    post_avro(ORDER_TOPIC, payload)


def create_random_order_for_customer(customer_id: str) -> OrderEvent:
    order = build_random_order(customer_id)
    publish_order(order)
    base_row = load_cached_customer(customer_id) or {"customer_id": customer_id}
    base_row["order_count"] = base_row.get("order_count", 0) + 1
    base_row["lifetime_value"] = round(base_row.get("lifetime_value", 0.0) + order.order_total, 2)
    base_row["last_order_ts"] = order.placed_at
    if not (base_row.get("first_name") or base_row.get("email")):
        profile_snapshot = wait_for_customer_profile(customer_id)
        if profile_snapshot:
            for key, value in profile_snapshot.items():
                if value is not None:
                    base_row[key] = value
    upsert_cached_customer(base_row)
    prime_customer_snapshot(customer_id)
    return order


def create_customer_with_order() -> Tuple[CustomerProfile, OrderEvent]:
    customer = build_random_customer()
    order = build_random_order(customer.customer_id)
    publish_customer_profile(customer)
    profile_snapshot = wait_for_customer_profile(customer.customer_id)
    publish_order(order)
    seed_row = {
        "customer_id": customer.customer_id,
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "lifecycle_stage": customer.lifecycle_stage,
        "is_active": customer.is_active,
        "order_count": 1,
        "lifetime_value": round(order.order_total, 2),
        "last_order_ts": order.placed_at,
    }
    if profile_snapshot:
        seed_row.update({k: v for k, v in profile_snapshot.items() if v is not None})
    upsert_cached_customer(seed_row)
    prime_customer_snapshot(customer.customer_id)
    return customer, order


def fetch_consolidated_rows(wait_seconds: float = 3.0) -> List[Dict]:
    """Retrieve consolidated rows from the Redis cache, waiting briefly for updates."""
    deadline = time.time() + wait_seconds
    while True:
        cached = load_cached_rows(RESULT_LIMIT)
        if cached:
            return cached
        if time.time() >= deadline:
            break
        time.sleep(0.2)

    if not get_redis_client():
        raise DataUnavailableError(
            "Redis cache unavailable. Ensure REDIS_URL points to a reachable Redis instance."
        )

    return []


@app.route("/", methods=["GET", "POST"])
def index():
    success_messages: List[str] = []
    error_messages: List[str] = []

    if request.method == "POST":
        action = request.form.get("action", "")
        if action == "order_existing":
            customer_id = (request.form.get("customer_id") or "").strip()
            if not customer_id:
                error_messages.append("Missing customer id for existing order request.")
            else:
                try:
                    order = create_random_order_for_customer(customer_id)
                    success_messages.append(
                        f"Published order {order.order_id} ({order.channel}, ${order.order_total:.2f}) "
                        f"for customer {customer_id}."
                    )
                except EventPublishError as exc:
                    error_messages.append(str(exc))
        elif action == "order_new":
            try:
                customer, order = create_customer_with_order()
                success_messages.append(
                    f"Created customer {customer.customer_id} ({customer.first_name} {customer.last_name}) "
                    f"and published order {order.order_id}."
                )
            except EventPublishError as exc:
                error_messages.append(str(exc))
        else:
            error_messages.append("Unknown action requested.")

    data_error = None
    customers: List[Dict] = []
    try:
        customers = fetch_consolidated_rows()
    except DataUnavailableError as exc:
        data_error = str(exc)

    return render_template(
        "index.html",
        customers=customers,
        success_messages=success_messages,
        error_messages=error_messages,
        data_error=data_error,
        result_limit=RESULT_LIMIT,
    )


reset_cache()
start_cache_sync_thread()


if __name__ == "__main__":
    debug_enabled = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug_enabled, host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
