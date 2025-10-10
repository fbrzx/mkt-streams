import json
import os
import time
from pathlib import Path
from typing import Dict

from kafka import KafkaConsumer

from fastavro import parse_schema


REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = REPO_ROOT / "schemas"
DELTA_DIR = REPO_ROOT / "delta"
CHECKPOINT_ROOT = Path(os.environ.get("SPARK_CHECKPOINT_ROOT", "/tmp/delta/checkpoints"))


def load_avro_schema(schema_name: str) -> Dict:
    """Load and parse an Avro schema into a Python dictionary."""
    schema_path = SCHEMA_DIR / f"{schema_name}.avsc"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema {schema_name} not found at {schema_path}")
    with schema_path.open() as f:
        schema = json.load(f)
    return parse_schema(schema)


def topic_config(default: str, env_var: str) -> str:
    return os.environ.get(env_var, default)


def kafka_bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def bronze_path(name: str) -> str:
    return str(DELTA_DIR / "bronze" / name)


def silver_path(name: str) -> str:
    return str(DELTA_DIR / "silver" / name)


def gold_path(name: str) -> str:
    return str(DELTA_DIR / "gold" / name)


def checkpoint_path(*parts: str) -> str:
    path = CHECKPOINT_ROOT.joinpath(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def wait_for_delta(path: str, timeout: int = 120, poll_interval: float = 2.0) -> None:
    """Block until a Delta table exists at `path` or raise after timeout."""
    target = Path(path) / "_delta_log"
    deadline = time.time() + timeout
    notified = False

    while time.time() < deadline:
        if target.exists() and any(target.iterdir()):
            return
        if not notified:
            print(f"Waiting for Delta table at {path}...", flush=True)
            notified = True
        time.sleep(poll_interval)

    raise TimeoutError(f"Delta log not found at {path} within {timeout} seconds")


def wait_for_topic(topic: str, timeout: int = 120, poll_interval: float = 2.0) -> None:
    """Block until the Kafka topic exists or raise after timeout."""
    bootstrap = kafka_bootstrap_servers()
    deadline = time.time() + timeout
    notified = False
    last_error = None

    while time.time() < deadline:
        try:
            consumer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=5000)
            partitions = consumer.partitions_for_topic(topic)
            consumer.close()
            if partitions is not None:
                return
        except Exception as exc:  # pragma: no cover - diagnostic logging only
            last_error = exc
        if not notified:
            msg = f"Waiting for Kafka topic {topic}..."
            if last_error:
                msg += f" (last error: {last_error})"
            print(msg, flush=True)
            notified = True
        time.sleep(poll_interval)

    raise TimeoutError(f"Topic {topic} not found within {timeout} seconds")
