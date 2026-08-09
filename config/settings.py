"""
Shared configuration for the E-Commerce Data Pipeline.

All environment-dependent settings are read from environment variables
with sensible defaults matching docker-compose.yml.

See .env.example for the full list of supported variables.

Usage:
    from config.settings import JAVA_HOME, POSTGRES_HOST, JDBC_URL, ...
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ──────────────────────────────────────────────
# Project Root (files/ directory)
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

try:
    from dotenv import load_dotenv
    _env_path = PROJECT_ROOT / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=False)
except ImportError:
    pass

# ──────────────────────────────────────────────
# Java 21 — required for PySpark 3.5.1
# ──────────────────────────────────────────────
JAVA_HOME = os.environ.get("JAVA_HOME")
if not JAVA_HOME:
    raise EnvironmentError(
        "JAVA_HOME is not set. Point it to your Java 21 installation.\n"
        "See .env.example for required environment variables."
    )
os.environ["JAVA_HOME"] = JAVA_HOME

# ──────────────────────────────────────────────
# Hadoop — required for PySpark on Windows
# ──────────────────────────────────────────────
HADOOP_HOME = os.environ.get("HADOOP_HOME", str(PROJECT_ROOT / "hadoop"))
os.environ["HADOOP_HOME"] = HADOOP_HOME

# Add hadoop/bin to PATH so JVM can find hadoop.dll for NativeIO
_hadoop_bin = str(Path(HADOOP_HOME) / "bin")
if _hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")

# ──────────────────────────────────────────────
# PySpark Python & UTF-8 Encoding for Windows
# ──────────────────────────────────────────────
_python = sys.executable
os.environ["PYSPARK_PYTHON"] = _python
os.environ["PYSPARK_DRIVER_PYTHON"] = _python
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PYTHONUTF8"] = "1"

# ──────────────────────────────────────────────
# PostgreSQL Connection
# ──────────────────────────────────────────────
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5433")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ecommerce")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin")
POSTGRES_SCHEMA = os.environ.get("POSTGRES_SCHEMA", "raw")

# JDBC URL for PySpark
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ──────────────────────────────────────────────
# AWS Data Lakehouse (S3 + Athena)
# ──────────────────────────────────────────────
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "ecommerce_raw")
ATHENA_S3_STAGING_DIR = os.environ.get("ATHENA_S3_STAGING_DIR", "")
ENABLE_AWS_LAKEHOUSE = os.environ.get("ENABLE_AWS_LAKEHOUSE", "false").lower() == "true"
