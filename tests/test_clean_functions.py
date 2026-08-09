"""
Unit tests for PySpark ETL clean functions.

Uses pytest with a local SparkSession (no Kafka, no Postgres, no S3).
Each test covers one clean_* function with known input/output rows.

Run:
    pytest tests/test_clean_functions.py -v
"""

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

# ── Project root must be on PYTHONPATH for imports ──────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Load project config (sets JAVA_HOME, HADOOP_HOME, PYSPARK_PYTHON) ─────
from config.settings import JAVA_HOME, HADOOP_HOME  # noqa: E402

if not os.environ.get("JAVA_HOME"):
    pytest.skip(
        "JAVA_HOME not set — skipping Spark tests. Set JAVA_HOME to run.",
        allow_module_level=True,
    )

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)


@pytest.fixture(scope="module")
def spark():
    """Minimal local SparkSession for unit testing — no YARN, no HDFS."""
    session = (
        SparkSession.builder
        .appName("test-clean-functions")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}")
        .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def create_test_df(spark, data: list, schema: StructType):
    """Create a PySpark DataFrame via JVM JSON reader to avoid Windows Python RDD worker IPC crashes."""
    field_names = [field.name for field in schema.fields]
    dict_data = []
    for row in data:
        if isinstance(row, dict):
            dict_data.append(row)
        else:
            dict_data.append(dict(zip(field_names, row)))

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(dict_data, tmp)
    tmp.close()

    df = spark.read.schema(schema).json(tmp.name)
    return df


# ─────────────────────────────────────────────────────────────────
# clean_orders
# ─────────────────────────────────────────────────────────────────

class TestCleanOrders:
    def test_drops_null_order_id(self, spark):
        from batch.spark_etl import clean_orders
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", StringType(), True),
            StructField("order_delivered_customer_date", StringType(), True),
            StructField("order_estimated_delivery_date", StringType(), True),
        ])
        data = [
            ("ord_001", "cust_001", "delivered", "2024-01-15 10:30:00", None, "2024-01-22 00:00:00"),
            (None,      "cust_002", "shipped",   "2024-01-16 11:00:00", None, "2024-01-25 00:00:00"),
        ]
        df = create_test_df(spark, data, schema)
        result = clean_orders(df)
        assert result.count() == 1
        assert result.collect()[0]["order_id"] == "ord_001"

    def test_drops_duplicate_order_id(self, spark):
        from batch.spark_etl import clean_orders
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", StringType(), True),
            StructField("order_delivered_customer_date", StringType(), True),
            StructField("order_estimated_delivery_date", StringType(), True),
        ])
        data = [
            ("ord_001", "cust_001", "delivered", "2024-01-15 10:30:00", None, "2024-01-22 00:00:00"),
            ("ord_001", "cust_001", "delivered", "2024-01-15 10:30:00", None, "2024-01-22 00:00:00"),
        ]
        df = create_test_df(spark, data, schema)
        result = clean_orders(df)
        assert result.count() == 1

    def test_selects_expected_columns(self, spark):
        from batch.spark_etl import clean_orders
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", StringType(), True),
            StructField("order_delivered_customer_date", StringType(), True),
            StructField("order_estimated_delivery_date", StringType(), True),
        ])
        data = [("ord_001", "cust_001", "delivered", "2024-01-15 10:30:00", None, "2024-01-22 00:00:00")]
        df = create_test_df(spark, data, schema)
        result = clean_orders(df)
        expected_cols = {
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        }
        assert set(result.columns) == expected_cols


# ─────────────────────────────────────────────────────────────────
# clean_customers
# ─────────────────────────────────────────────────────────────────

class TestCleanCustomers:
    def _schema(self):
        return StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_unique_id", StringType(), True),
            StructField("customer_city", StringType(), True),
            StructField("customer_state", StringType(), True),
        ])

    def test_drops_null_customer_id(self, spark):
        from batch.spark_etl import clean_customers
        data = [
            ("cust_001", "uniq_001", "sao paulo", "SP"),
            (None,       "uniq_002", "rio",        "RJ"),
        ]
        df = create_test_df(spark, data, self._schema())
        result = clean_customers(df)
        assert result.count() == 1

    def test_trims_whitespace(self, spark):
        from batch.spark_etl import clean_customers
        data = [("cust_001", "uniq_001", "  sao paulo  ", " SP ")]
        df = create_test_df(spark, data, self._schema())
        result = clean_customers(df)
        row = result.collect()[0]
        assert row["customer_city"] == "sao paulo"
        assert row["customer_state"] == "SP"

    def test_deduplicates_on_customer_id(self, spark):
        from batch.spark_etl import clean_customers
        data = [
            ("cust_001", "uniq_001", "sao paulo", "SP"),
            ("cust_001", "uniq_001", "sao paulo", "SP"),
        ]
        df = create_test_df(spark, data, self._schema())
        result = clean_customers(df)
        assert result.count() == 1


# ─────────────────────────────────────────────────────────────────
# clean_products
# ─────────────────────────────────────────────────────────────────

class TestCleanProducts:
    def _schema(self):
        return StructType([
            StructField("product_id", StringType(), True),
            StructField("product_category_name", StringType(), True),
        ])

    def test_fills_null_category_with_unknown(self, spark):
        from batch.spark_etl import clean_products
        data = [("prod_001", None), ("prod_002", "electronics")]
        df = create_test_df(spark, data, self._schema())
        result = clean_products(df)
        rows = {r["product_id"]: r["product_category_name"] for r in result.collect()}
        assert rows["prod_001"] == "unknown"
        assert rows["prod_002"] == "electronics"

    def test_drops_null_product_id(self, spark):
        from batch.spark_etl import clean_products
        data = [(None, "electronics"), ("prod_001", "furniture")]
        df = create_test_df(spark, data, self._schema())
        result = clean_products(df)
        assert result.count() == 1
        assert result.collect()[0]["product_id"] == "prod_001"


# ─────────────────────────────────────────────────────────────────
# clean_order_items
# ─────────────────────────────────────────────────────────────────

class TestCleanOrderItems:
    def _schema(self):
        return StructType([
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("price", StringType(), True),         # raw CSV = string
            StructField("freight_value", StringType(), True),
        ])

    def test_drops_null_order_id(self, spark):
        from batch.spark_etl import clean_order_items
        data = [
            ("ord_001", 1, "prod_001", "seller_001", "199.90", "15.50"),
            (None,      1, "prod_001", "seller_001", "99.00",  "10.00"),
        ]
        df = create_test_df(spark, data, self._schema())
        result = clean_order_items(df)
        assert result.count() == 1

    def test_casts_price_to_double(self, spark):
        from batch.spark_etl import clean_order_items
        data = [("ord_001", 1, "prod_001", "seller_001", "199.90", "15.50")]
        df = create_test_df(spark, data, self._schema())
        result = clean_order_items(df)
        assert result.schema["price"].dataType == DoubleType()

    def test_deduplicates_on_order_item_grain(self, spark):
        from batch.spark_etl import clean_order_items
        data = [
            ("ord_001", 1, "prod_001", "seller_001", "199.90", "15.50"),
            ("ord_001", 1, "prod_001", "seller_001", "199.90", "15.50"),  # duplicate
        ]
        df = create_test_df(spark, data, self._schema())
        result = clean_order_items(df)
        assert result.count() == 1
