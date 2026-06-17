"""
Kafka Producer: Simulates live e-commerce order events.

Generates random order events and publishes them to the
'orders_stream' Kafka topic at ~30 events/minute.

Usage:
    python streaming/kafka_producer.py
"""

import json
import time
import random
import string
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streaming.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, LOG_DIR

from kafka import KafkaProducer

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "kafka_producer.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("kafka_producer")

# ──────────────────────────────────────────────
# Reference Data (pools for random selection)
# ──────────────────────────────────────────────
PRODUCT_IDS = [f"P{str(i).zfill(4)}" for i in range(1, 101)]
CUSTOMER_IDS = [f"C{str(i).zfill(4)}" for i in range(1, 501)]


def generate_order() -> dict:
    """
    Generate a single random order event.

    Returns a dict with:
        order_id     — unique identifier (e.g. ORD-A3F8K2B1)
        customer_id  — random from pool (e.g. C0042)
        product_id   — random from pool (e.g. P0007)
        amount       — random float 9.99–999.99
        quantity     — random int 1–5
        event_time   — current UTC time in ISO-8601
    """
    order_id = "ORD-" + "".join(
        random.choices(string.ascii_uppercase + string.digits, k=8)
    )
    return {
        "order_id": order_id,
        "customer_id": random.choice(CUSTOMER_IDS),
        "product_id": random.choice(PRODUCT_IDS),
        "amount": round(random.uniform(9.99, 999.99), 2),
        "quantity": random.randint(1, 5),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def on_send_success(record_metadata):
    """Callback for successful message delivery."""
    logger.debug(
        "Delivered to %s [partition %d] @ offset %d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(excp):
    """Callback for failed message delivery."""
    logger.error("Failed to deliver message", exc_info=excp)


def main():
    """
    Main producer loop.
    Connects to Kafka and sends one order event every ~2 seconds.
    """
    logger.info("=" * 60)
    logger.info("Kafka Producer Starting")
    logger.info("  Bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic:     %s", KAFKA_TOPIC)
    logger.info("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=500,
    )

    events_sent = 0

    try:
        while True:
            order = generate_order()
            producer.send(KAFKA_TOPIC, value=order).add_callback(
                on_send_success
            ).add_errback(on_send_error)

            events_sent += 1

            if events_sent % 10 == 0:
                logger.info(
                    "Events sent: %d | Latest: %s (amount=%.2f, qty=%d)",
                    events_sent,
                    order["order_id"],
                    order["amount"],
                    order["quantity"],
                )

            time.sleep(2)  # ~30 events/minute

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed. Total events sent: %d", events_sent)


if __name__ == "__main__":
    main()
