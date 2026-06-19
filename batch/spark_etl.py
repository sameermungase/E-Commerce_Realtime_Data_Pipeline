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

import sys
from pathlib import Path

# Ensure batch/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch.config import (
    JAVA_HOME,
    CSV_FILES,
    JDBC_URL,
    JDBC_DRIVER_PATH,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    RAW_TABLES,
    GCP_PROJECT,
    BQ_RAW_TABLES,
    ENABLE_BIGQUERY,
)

# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType


# ----------------------------------------------
# Spark Session
# ----------------------------------------------

def create_spark_session() -> SparkSession:
    """Create a SparkSession with PostgreSQL JDBC driver (and optionally BigQuery connector) on classpath."""
    builder = (
        SparkSession.builder
        .appName("ecommerce-etl")
        .master("local[*]")
        .config("spark.driver.extraClassPath", str(JDBC_DRIVER_PATH))
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    )

    # Add BigQuery connector when enabled
    if ENABLE_BIGQUERY:
        builder = builder.config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
        )
        print("[CONFIG] BigQuery connector enabled")

    return builder.getOrCreate()


# ----------------------------------------------
# Read CSVs
# ----------------------------------------------

def read_csv(spark: SparkSession, name: str) -> DataFrame:
    """Read a CSV file with header and inferred schema."""
    path = str(CSV_FILES[name])
    print(f"[READ] Reading {name} from {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)
    print(f"   -> {df.count()} rows, {len(df.columns)} columns")
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
    print(f"   -> {df.count()} rows after cleaning")
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
    print(f"   -> {df.count()} rows after cleaning")
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
    print(f"   -> {df.count()} rows after cleaning")
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
    print(f"   -> {df.count()} rows after cleaning")
    return df


# ----------------------------------------------
# Load to PostgreSQL
# ----------------------------------------------

def write_to_postgres(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame to PostgreSQL via JDBC, overwriting existing data."""
    print(f"[WRITE] Writing to PostgreSQL table: {table_name}")
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
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
# Load to BigQuery (GCP Cloud Warehouse)
# ----------------------------------------------

def write_to_bigquery(df: DataFrame, table_name: str) -> None:
    """
    Write a DataFrame to Google BigQuery using the spark-bigquery-connector.

    Uses Application Default Credentials (ADC) — requires either:
      - `gcloud auth application-default login` (local dev)
      - Service account key via GOOGLE_APPLICATION_CREDENTIALS env var

    Args:
        df: The cleaned DataFrame to write.
        table_name: Fully qualified BigQuery table (dataset.table).
    """
    print(f"[WRITE] Writing to BigQuery table: {GCP_PROJECT}.{table_name}")
    (
        df.write
        .format("bigquery")
        .option("table", f"{GCP_PROJECT}.{table_name}")
        .option("temporaryGcsBucket", f"{GCP_PROJECT}-spark-temp")
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )
    print(f"   [OK] {GCP_PROJECT}.{table_name} written successfully")


# ----------------------------------------------
# Main Pipeline
# ----------------------------------------------

def main():
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

        # -- Load to BigQuery (if enabled) --
        if ENABLE_BIGQUERY:
            print()
            print("=" * 60)
            print(">>> Loading to BigQuery (GCP Cloud Warehouse)")
            print("=" * 60)
            write_to_bigquery(orders, BQ_RAW_TABLES["orders"])
            write_to_bigquery(customers, BQ_RAW_TABLES["customers"])
            write_to_bigquery(products, BQ_RAW_TABLES["products"])
            write_to_bigquery(order_items, BQ_RAW_TABLES["order_items"])

        print()
        print("=" * 60)
        print("[OK] ETL Pipeline Complete -- All raw tables loaded!")
        if ENABLE_BIGQUERY:
            print("     ✅ PostgreSQL + BigQuery")
        else:
            print("     ✅ PostgreSQL (BigQuery disabled — set ENABLE_BIGQUERY=true)")
        print("=" * 60)

    except Exception as e:
        print(f"[ERROR] ETL Pipeline Failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
