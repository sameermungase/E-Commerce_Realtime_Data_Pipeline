"""
Dead Letter Queue handler for the streaming pipeline.

Invalid records that fail data quality checks are saved to
JSON files in the bad_records/ directory for later inspection
and reprocessing.

This demonstrates production thinking — rather than silently
dropping bad data, we capture it for observability and debugging.
"""

import json
import os
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streaming.config import BAD_RECORDS_DIR

logger = logging.getLogger("dead_letter")


def save_bad_records(batch_df, batch_id: int, reason: str) -> None:
    """
    Save invalid records from a streaming micro-batch to disk.

    Records are written as a JSON file named by batch_id and timestamp
    so they can be traced back to a specific streaming batch.

    Args:
        batch_df: Spark DataFrame containing the bad records.
        batch_id: The micro-batch identifier from Spark.
        reason:   Human-readable reason why records were rejected.
    """
    os.makedirs(BAD_RECORDS_DIR, exist_ok=True)

    # Collect to driver — safe because bad records should be few
    records = [row.asDict() for row in batch_df.collect()]

    if not records:
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"batch_{batch_id}_{timestamp}.json"
    filepath = os.path.join(BAD_RECORDS_DIR, filename)

    payload = {
        "batch_id": batch_id,
        "reason": reason,
        "record_count": len(records),
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    logger.warning(
        "Batch %d: %d bad records saved to %s (reason: %s)",
        batch_id,
        len(records),
        filepath,
        reason,
    )
