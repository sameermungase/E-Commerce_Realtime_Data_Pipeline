"""
PostgreSQL JDBC sink for Spark Structured Streaming.

Provides the `write_to_postgres` function used as a foreachBatch
sink. Each micro-batch is upserted to streaming.raw_orders_stream
via JDBC with ON CONFLICT DO NOTHING to ensure idempotency.

Why foreachBatch?
    - Better control over write logic and error handling
    - Supports retries per batch
    - Allows custom sink logic (e.g. dead-letter routing)
    - This is the recommended pattern for JDBC in Spark Streaming

Why ON CONFLICT DO NOTHING?
    - Spark Structured Streaming provides at-least-once delivery.
    - Without conflict handling, a batch retry (e.g. after task failure)
      would insert duplicate rows into the landing table.
    - Idempotent inserts ensure exactly-once effective writes.
"""

import logging
import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streaming.config import (  # noqa: E402
    JDBC_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    STREAMING_TABLE,
)

logger = logging.getLogger("postgres_sink")

# Idempotent insert statement — skips rows whose order_id already exists
# Requires the target table to have a UNIQUE or PRIMARY KEY on order_id.
_INSERT_SQL = (
    f"INSERT INTO {STREAMING_TABLE} "
    f"(order_id, customer_id, product_id, amount, quantity, "
    f"total_value, event_time, processing_time) "
    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
    f"ON CONFLICT (order_id) DO NOTHING"
)


def write_to_postgres(batch_df, batch_id: int) -> None:
    """
    Write a Spark Structured Streaming micro-batch to PostgreSQL.

    Uses ON CONFLICT DO NOTHING to make writes idempotent — safe
    to retry on failure without producing duplicate rows.

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
            # Idempotent upsert: skip rows that already exist (at-least-once → exactly-once effective)
            .option("insertStatement", _INSERT_SQL)
            .mode("append")
            .save()
        )
        logger.info("Batch %d: [OK] %d records written successfully", batch_id, record_count)

    except Exception as e:
        logger.error("Batch %d: [FAIL] Write failed — %s", batch_id, e)
        raise
