-- ================================================================
-- Initialize schemas for the data warehouse
-- ================================================================
-- raw:       landing zone for PySpark batch-loaded data
-- analytics: dbt-built star schema models
-- streaming: landing zone for Spark Structured Streaming data
-- ================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS streaming;

-- ================================================================
-- Streaming: raw_orders_stream
-- Written by Spark Structured Streaming via foreachBatch + JDBC
-- ================================================================

CREATE TABLE IF NOT EXISTS streaming.raw_orders_stream (
    order_id        VARCHAR(50)      PRIMARY KEY,
    customer_id     VARCHAR(50)      NOT NULL,
    product_id      VARCHAR(50)      NOT NULL,
    amount          DOUBLE PRECISION NOT NULL,
    quantity        INTEGER          NOT NULL,
    total_value     DOUBLE PRECISION NOT NULL,
    event_time      TIMESTAMP        NOT NULL,
    processing_time TIMESTAMP        NOT NULL
);

-- Indexes for analytical queries
CREATE INDEX IF NOT EXISTS idx_stream_event_time
    ON streaming.raw_orders_stream (event_time);

CREATE INDEX IF NOT EXISTS idx_stream_customer_id
    ON streaming.raw_orders_stream (customer_id);

CREATE INDEX IF NOT EXISTS idx_stream_product_id
    ON streaming.raw_orders_stream (product_id);
