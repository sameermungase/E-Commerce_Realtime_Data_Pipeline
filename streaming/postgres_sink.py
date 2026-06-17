"""
PostgreSQL JDBC sink for Spark Structured Streaming.

Provides the `write_to_postgres` function used as a foreachBatch
sink. Each micro-batch is appended to the streaming.raw_orders_stream
table via JDBC.

Why foreachBatch?
    - Better control over write logic and error handling
    - Supports retries per batch
    - Allows custom sink logic (e.g. dead-letter routing)
    - This is the recommended pattern for JDBC in Spark Streaming
"""

import logging
import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streaming.config import (
    JDBC_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    STREAMING_TABLE,
)

logger = logging.getLogger("postgres_sink")


def write_to_postgres(batch_df, batch_id: int) -> None:
    """
    Write a Spark Structured Streaming micro-batch to PostgreSQL.

    Args:
        batch_df: The micro-batch DataFrame from Spark.
        batch_id: The unique batch identifier assigned by Spark.
    """
    record_count = batch_df.count()

    if record_count == 0:
        logger.info("Batch %d: empty — skipping write", batch_id)
        return

    logger.info("Batch %d: writing %d records to %s", batch_id, record_count, STREAMING_TABLE)

    try:
        (
            batch_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", STREAMING_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        logger.info("Batch %d: ✅ %d records written successfully", batch_id, record_count)

    except Exception as e:
        logger.error("Batch %d: ❌ Write failed — %s", batch_id, e)
        raise
