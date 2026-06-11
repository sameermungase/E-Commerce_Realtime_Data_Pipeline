"""
Centralized configuration for the E-Commerce ETL pipeline.

All paths, database credentials, and environment settings are defined here
so that spark_etl.py and other scripts can import them cleanly.
"""

import os
from pathlib import Path

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

# ──────────────────────────────────────────────
# Dataset Paths
# ──────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / "data" / "olist"

CSV_FILES = {
    "orders": DATA_DIR / "olist_orders_dataset.csv",
    "order_items": DATA_DIR / "olist_order_items_dataset.csv",
    "customers": DATA_DIR / "olist_customers_dataset.csv",
    "products": DATA_DIR / "olist_products_dataset.csv",
}

# ──────────────────────────────────────────────
# PostgreSQL Connection
# ──────────────────────────────────────────────
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5433
POSTGRES_DB = "ecommerce"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin"
POSTGRES_SCHEMA = "raw"

# JDBC URL for PySpark
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# JDBC driver JAR path
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
