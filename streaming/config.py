"""
Centralized configuration for the streaming pipeline.

All settings use environment variables with sensible defaults
so the pipeline works locally and in Docker without code changes.
"""

import os
import sys
from pathlib import Path

# ──────────────────────────────────────────────
# PySpark Python — Windows has no 'python3'
# ──────────────────────────────────────────────
_python = sys.executable
os.environ.setdefault("PYSPARK_PYTHON", _python)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", _python)

# ──────────────────────────────────────────────
# Project Root (files/ directory)
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ──────────────────────────────────────────────
# Java 21 — required for PySpark 3.5.1
# ──────────────────────────────────────────────
JAVA_HOME = r"C:\Users\snowp\OneDrive\Desktop\Projects\DS_project\Java"
os.environ["JAVA_HOME"] = JAVA_HOME

# ──────────────────────────────────────────────
# Hadoop — required for PySpark on Windows
# ──────────────────────────────────────────────
HADOOP_HOME = str(PROJECT_ROOT / "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_HOME
# Add hadoop/bin to PATH so JVM can find hadoop.dll for NativeIO
_hadoop_bin = str(PROJECT_ROOT / "hadoop" / "bin")
os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")

# ──────────────────────────────────────────────
# Kafka
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "orders_stream")

# ──────────────────────────────────────────────
# PostgreSQL (JDBC for Spark)
# ──────────────────────────────────────────────
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5433")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ecommerce")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

STREAMING_TABLE = "streaming.raw_orders_stream"

# ──────────────────────────────────────────────
# Spark Streaming
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
