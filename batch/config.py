"""
Batch pipeline configuration.

Imports shared settings from config.settings and adds batch-specific
paths (CSV files, raw table names, S3 lakehouse paths, JDBC driver JAR).
"""

import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (  # noqa: E402
    PROJECT_ROOT,
    POSTGRES_SCHEMA,
    ENABLE_AWS_LAKEHOUSE,
    S3_BUCKET_NAME,
    ATHENA_DATABASE,
    AWS_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    JAVA_HOME,
    JDBC_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# Re-export shared settings so existing imports in spark_etl.py keep working
__all__ = [
    "PROJECT_ROOT", "JAVA_HOME", "JDBC_URL",
    "POSTGRES_USER", "POSTGRES_PASSWORD",
    "ENABLE_AWS_LAKEHOUSE", "S3_BUCKET_NAME", "ATHENA_DATABASE",
    "AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
    "CSV_FILES", "RAW_TABLES", "S3_RAW_PATHS", "JDBC_DRIVER_PATH",
]

# ──────────────────────────────────────────────
# Dataset Paths
# ──────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / "data" / "olist"
if not DATA_DIR.exists() or not any(DATA_DIR.glob("*.csv")):
    DATA_DIR = PROJECT_ROOT / "tests" / "fixtures"

CSV_FILES = {
    "orders": DATA_DIR / "olist_orders_dataset.csv",
    "order_items": DATA_DIR / "olist_order_items_dataset.csv",
    "customers": DATA_DIR / "olist_customers_dataset.csv",
    "products": DATA_DIR / "olist_products_dataset.csv",
}

# ──────────────────────────────────────────────
# JDBC driver JAR path
# ──────────────────────────────────────────────
JDBC_DRIVER_PATH = PROJECT_ROOT / "batch" / "jars" / "postgresql-42.7.3.jar"

# ──────────────────────────────────────────────
# Raw Table Names (written by PySpark)
# ──────────────────────────────────────────────
RAW_TABLES = {
    "orders": f"{POSTGRES_SCHEMA}.orders",
    "order_items": f"{POSTGRES_SCHEMA}.order_items",
    "customers": f"{POSTGRES_SCHEMA}.customers",
    "products": f"{POSTGRES_SCHEMA}.products",
}

# ──────────────────────────────────────────────
# Amazon S3 Lakehouse Paths (Parquet format)
# ──────────────────────────────────────────────
S3_RAW_PATHS = {
    "orders": f"s3a://{S3_BUCKET_NAME}/raw/orders/",
    "order_items": f"s3a://{S3_BUCKET_NAME}/raw/order_items/",
    "customers": f"s3a://{S3_BUCKET_NAME}/raw/customers/",
    "products": f"s3a://{S3_BUCKET_NAME}/raw/products/",
}

