"""
PySpark ETL Pipeline - E-Commerce Data (Olist)

Reads 4 CSV files, cleans each independently, and loads them
into PostgreSQL as separate raw tables:
    - raw.orders
    - raw.customers
    - raw.products
    - raw.order_items

dbt handles all joins and star-schema modeling downstream.
"""

import os
import sys
from pathlib import Path

# Ensure batch/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch.config import (  # noqa: E402
    CSV_FILES,
    JDBC_URL,
    JDBC_DRIVER_PATH,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    RAW_TABLES,
    ENABLE_AWS_LAKEHOUSE,
    S3_BUCKET_NAME,
    S3_RAW_PATHS,
    AWS_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
)

from pyspark.sql import SparkSession, DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import TimestampType, DoubleType  # noqa: E402

# ──────────────────────────────────────────────
# Debug helper: avoid eager .count() in production
# ──────────────────────────────────────────────
_DEBUG = os.environ.get("ETL_DEBUG", "false").lower() == "true"


def _row_count(df: DataFrame, label: str) -> None:
    """Log row count only when ETL_DEBUG=true; otherwise log a lightweight note."""
    if _DEBUG:
        print(f"   -> {df.count():,} rows {label}")
    else:
        print("   -> (row count skipped - set ETL_DEBUG=true to enable)")


# ----------------------------------------------
# Spark Session
# ----------------------------------------------

def create_spark_session() -> SparkSession:
    """Create a SparkSession with PostgreSQL JDBC driver (and optionally hadoop-aws for S3) on classpath."""
    python_exe = sys.executable
    builder = (
        SparkSession.builder
        .appName("ecommerce-etl")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        .config("spark.driver.extraClassPath", str(JDBC_DRIVER_PATH))
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    )

    # Add hadoop-aws connector for S3 access when enabled
    if ENABLE_AWS_LAKEHOUSE:
        builder = (
            builder
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
        print("[CONFIG] AWS S3 Lakehouse connector enabled")

    return builder.getOrCreate()


# ----------------------------------------------
# Read CSVs
# ----------------------------------------------

def read_csv(spark: SparkSession, name: str) -> DataFrame:
    """Read a CSV file with header and inferred schema."""
    path = str(CSV_FILES[name])
    print(f"[READ] Reading {name} from {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)
    _row_count(df, "read")
    return df


# ----------------------------------------------
# Clean Functions (one per table)
# ----------------------------------------------

def clean_orders(df: DataFrame) -> DataFrame:
    """
    Clean orders table:
    - Drop duplicates on order_id
    - Drop rows with null order_id or customer_id
    - Cast order_purchase_timestamp to TimestampType
    - Select relevant columns
    """
    print("[CLEAN] Cleaning orders...")
    df = (
        df
        .dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .withColumn(
            "order_purchase_timestamp",
            F.col("order_purchase_timestamp").cast(TimestampType())
        )
        .withColumn(
            "order_delivered_customer_date",
            F.col("order_delivered_customer_date").cast(TimestampType())
        )
        .withColumn(
            "order_estimated_delivery_date",
            F.col("order_estimated_delivery_date").cast(TimestampType())
        )
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        )
    )
    _row_count(df, "after cleaning orders")
    return df


def clean_customers(df: DataFrame) -> DataFrame:
    """
    Clean customers table:
    - Drop duplicates on customer_id
    - Drop rows with null customer_id
    - Trim city and state strings
    """
    print("[CLEAN] Cleaning customers...")
    df = (
        df
        .dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
        .withColumn("customer_city", F.trim(F.col("customer_city")))
        .withColumn("customer_state", F.trim(F.col("customer_state")))
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
        )
    )
    _row_count(df, "after cleaning customers")
    return df


def clean_products(df: DataFrame) -> DataFrame:
    """
    Clean products table:
    - Drop duplicates on product_id
    - Drop rows with null product_id
    - Fill null category names with 'unknown'
    """
    print("[CLEAN] Cleaning products...")
    df = (
        df
        .dropDuplicates(["product_id"])
        .filter(F.col("product_id").isNotNull())
        .fillna({"product_category_name": "unknown"})
        .select(
            "product_id",
            "product_category_name",
        )
    )
    _row_count(df, "after cleaning products")
    return df


def clean_order_items(df: DataFrame) -> DataFrame:
    """
    Clean order_items table:
    - Drop duplicates on (order_id, order_item_id)
    - Drop rows with null order_id or product_id
    - Cast price and freight_value to DoubleType
    """
    print("[CLEAN] Cleaning order_items...")
    df = (
        df
        .dropDuplicates(["order_id", "order_item_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
        )
    )
    _row_count(df, "after cleaning order_items")
    return df


# ----------------------------------------------
# Load to PostgreSQL
# ----------------------------------------------

def write_to_postgres(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame to PostgreSQL via JDBC, overwriting existing data."""
    print(f"[WRITE] Writing to PostgreSQL table: {table_name}")
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("sessionInitStatement", "SET TIME ZONE 'UTC'")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print(f"   [OK] {table_name} written successfully")


# ----------------------------------------------
# Load to Amazon S3 Lakehouse (Parquet)
# ----------------------------------------------

def write_to_s3_lakehouse(df: DataFrame, s3_path: str) -> None:
    """
    Write a DataFrame to Amazon S3 in Snappy-compressed Parquet format.

    Requires AWS credentials configured via environment variables:
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY

    Args:
        df: The cleaned DataFrame to write.
        s3_path: S3A path (e.g. s3a://<bucket>/raw/<table_name>/).
    """
    print(f"[WRITE] Writing to S3 Lakehouse: {s3_path}")
    (
        df.write
        .format("parquet")
        .option("compression", "snappy")
        .mode("overwrite")
        .save(s3_path)
    )
    print(f"   [OK] {s3_path} written successfully")


# ----------------------------------------------
# Main Pipeline
# ----------------------------------------------

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    print("=" * 60)
    print(">>> E-Commerce ETL Pipeline -- Starting")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # -- Read --
        orders_raw = read_csv(spark, "orders")
        customers_raw = read_csv(spark, "customers")
        products_raw = read_csv(spark, "products")
        order_items_raw = read_csv(spark, "order_items")

        # -- Clean --
        orders = clean_orders(orders_raw)
        customers = clean_customers(customers_raw)
        products = clean_products(products_raw)
        order_items = clean_order_items(order_items_raw)

        # -- Load to PostgreSQL --
        write_to_postgres(orders, RAW_TABLES["orders"])
        write_to_postgres(customers, RAW_TABLES["customers"])
        write_to_postgres(products, RAW_TABLES["products"])
        write_to_postgres(order_items, RAW_TABLES["order_items"])

        # -- Load to Amazon S3 Lakehouse (if enabled) --
        if ENABLE_AWS_LAKEHOUSE:
            print()
            print("=" * 60)
            print(">>> Loading to Amazon S3 Lakehouse (Parquet)")
            print("=" * 60)
            write_to_s3_lakehouse(orders, S3_RAW_PATHS["orders"])
            write_to_s3_lakehouse(customers, S3_RAW_PATHS["customers"])
            write_to_s3_lakehouse(products, S3_RAW_PATHS["products"])
            write_to_s3_lakehouse(order_items, S3_RAW_PATHS["order_items"])

        print()
        print("=" * 60)
        print("[OK] ETL Pipeline Complete -- All raw tables loaded!")
        if ENABLE_AWS_LAKEHOUSE:
            print("     ✅ PostgreSQL + Amazon S3 Lakehouse")
        else:
            print("     ✅ PostgreSQL (AWS Lakehouse disabled — set ENABLE_AWS_LAKEHOUSE=true)")
        print("=" * 60)

    except Exception as e:
        print(f"[ERROR] ETL Pipeline Failed: {e}")
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
