"""
Spark Structured Streaming Consumer.

Reads order events from the Kafka 'orders_stream' topic, applies
data quality filters and transformations, and writes valid records
to PostgreSQL via foreachBatch. Invalid records are routed to a
dead-letter queue.

Usage:
    python streaming/spark_streaming.py

Architecture:
    Kafka (orders_stream)
        → Spark readStream
        → Parse JSON with explicit schema
        → Transform (total_value, processing_time)
        → Filter (data quality checks)
        → Watermark + Dedup
        → foreachBatch → PostgreSQL (streaming.raw_orders_stream)
"""

import logging
import os
import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streaming.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CHECKPOINT_DIR,
    LOG_DIR,
    JDBC_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    STREAMING_TABLE,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from streaming.schema import ORDER_STREAM_SCHEMA
from streaming.dead_letter import save_bad_records

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "spark_streaming.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("spark_streaming")


# ──────────────────────────────────────────────
# Spark Session
# ──────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Create a SparkSession configured for Kafka + PostgreSQL.

    Includes:
        - spark-sql-kafka connector for Kafka readStream
        - PostgreSQL JDBC driver for foreachBatch writes
        - UTC timezone for consistent timestamp handling
    """
    from streaming.config import HADOOP_HOME

    hadoop_bin = str(Path(HADOOP_HOME) / "bin")
    python_exe = sys.executable

    return (
        SparkSession.builder
        .appName("ecommerce-streaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3",
        )
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager",
        )
        .config("spark.driver.extraLibraryPath", hadoop_bin)
        .config(
            "spark.driver.extraJavaOptions",
            "-Duser.timezone=UTC -Dhadoop.home.dir=" + HADOOP_HOME.replace("\\", "/"),
        )
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# foreachBatch Sink (with dead-letter routing)
# ──────────────────────────────────────────────

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Process a single micro-batch from Spark Structured Streaming.

    1. Separate valid and invalid records
    2. Write valid records to PostgreSQL
    3. Route invalid records to dead-letter queue

    Args:
        batch_df: The micro-batch DataFrame.
        batch_id: The batch identifier assigned by Spark.
    """
    if len(batch_df.head(1)) == 0:
        logger.info("Batch %d: empty — skipping", batch_id)
        return

    total = batch_df.count()
    logger.info("Batch %d: received %d records", batch_id, total)

    # ── Separate valid vs invalid records ────────────
    valid_df = batch_df.filter(
        (F.col("amount") > 0)
        & (F.col("quantity") > 0)
        & F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
    )

    invalid_df = batch_df.subtract(valid_df)
    invalid_count = invalid_df.count()

    if invalid_count > 0:
        logger.warning(
            "Batch %d: %d invalid records detected — routing to dead letter",
            batch_id,
            invalid_count,
        )
        save_bad_records(invalid_df, batch_id, "Failed data quality checks")

    # ── Write valid records to PostgreSQL ────────────
    valid_count = valid_df.count()
    if valid_count == 0:
        logger.warning("Batch %d: no valid records to write", batch_id)
        return

    logger.info("Batch %d: writing %d valid records to %s", batch_id, valid_count, STREAMING_TABLE)

    try:
        (
            valid_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", STREAMING_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        logger.info("Batch %d: ✅ %d records written successfully", batch_id, valid_count)
    except Exception as e:
        logger.error("Batch %d: ❌ Write failed — %s", batch_id, e)
        raise


# ──────────────────────────────────────────────
# Main Streaming Pipeline
# ──────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Spark Structured Streaming Consumer — Starting")
    logger.info("  Kafka:      %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic:      %s", KAFKA_TOPIC)
    logger.info("  Sink:       %s", STREAMING_TABLE)
    logger.info("  Checkpoint: %s", CHECKPOINT_DIR)
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # ── Step 1: Read from Kafka ──────────────────
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        logger.info("Connected to Kafka — readStream active")

        # ── Step 2: Parse JSON payload ───────────────
        parsed = (
            raw_stream
            .selectExpr("CAST(value AS STRING) AS json_str")
            .select(
                F.from_json(F.col("json_str"), ORDER_STREAM_SCHEMA).alias("data")
            )
            .select("data.*")
        )

        # ── Step 3: Transform ────────────────────────
        transformed = (
            parsed
            # Cast event_time from ISO string to proper Timestamp
            .withColumn("event_time", F.to_timestamp(F.col("event_time")))
            # Derived columns
            .withColumn("total_value", F.round(F.col("amount") * F.col("quantity"), 2))
            .withColumn("processing_time", F.current_timestamp())
            # Watermark for late-arriving data (common interview topic!)
            .withWatermark("event_time", "10 minutes")
            # Deduplicate by order_id within the watermark window
            .dropDuplicates(["order_id"])
        )

        # ── Step 4: Write via foreachBatch ───────────
        query = (
            transformed.writeStream
            .outputMode("append")
            .foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(processingTime="30 seconds")
            .queryName("orders_stream_to_postgres")
            .start()
        )

        logger.info("Streaming query started — awaiting termination (Ctrl+C to stop)")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error("Streaming pipeline failed: %s", e, exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
