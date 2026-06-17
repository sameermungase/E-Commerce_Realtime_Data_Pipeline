"""
Schema definitions for streaming data.

Defines the PySpark StructType used to parse JSON messages
from the Kafka 'orders_stream' topic.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

# Schema matching the JSON produced by kafka_producer.py
# event_time arrives as an ISO-8601 string and is cast to
# TimestampType during the streaming transformation step.
ORDER_STREAM_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("event_time", StringType(), nullable=False),
])
