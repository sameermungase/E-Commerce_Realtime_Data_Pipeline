"""
Streaming pipeline configuration.

Imports shared settings from config.settings and adds streaming-specific
settings (Kafka, checkpoints, dead-letter queue, logging).
"""

import os
import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (  # noqa: E402
    PROJECT_ROOT,
    HADOOP_HOME,
    JDBC_URL,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# Re-export shared settings so existing imports in spark_streaming.py keep working
__all__ = [
    "PROJECT_ROOT", "HADOOP_HOME", "JDBC_URL",
    "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "POSTGRES_USER", "POSTGRES_PASSWORD",
    "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC",
    "STREAMING_TABLE", "CHECKPOINT_DIR", "BAD_RECORDS_DIR", "LOG_DIR",
]

# ──────────────────────────────────────────────
# Kafka
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "orders_stream")

# ──────────────────────────────────────────────
# Streaming Table
# ──────────────────────────────────────────────
STREAMING_TABLE = "streaming.raw_orders_stream"

# ──────────────────────────────────────────────
# Spark Streaming Checkpoints
# ──────────────────────────────────────────────
CHECKPOINT_DIR = os.environ.get(
    "CHECKPOINT_DIR",
    str(PROJECT_ROOT / "streaming" / "checkpoints" / "orders_stream"),
)

# ──────────────────────────────────────────────
# Bad Records (Dead Letter Queue)
# ──────────────────────────────────────────────
BAD_RECORDS_DIR = os.environ.get(
    "BAD_RECORDS_DIR",
    str(PROJECT_ROOT / "bad_records"),
)

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
LOG_DIR = os.environ.get(
    "LOG_DIR",
    str(PROJECT_ROOT / "logs"),
)
