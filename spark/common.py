import json
import os
from pathlib import Path
from typing import Dict

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
