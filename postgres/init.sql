-- ================================================================
-- Initialize schemas for the data warehouse
-- ================================================================
-- raw:       landing zone for PySpark batch-loaded data
-- staging:   dbt staging models (views/tables)
-- analytics: dbt-built star schema models
-- streaming: landing zone for Spark Structured Streaming data
-- ================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS streaming;

-- ================================================================
-- Raw: batch-loaded tables (PySpark ETL in production, seed in CI)
-- ================================================================
-- These tables mirror the Olist CSV structure after PySpark cleaning.
-- In CI, minimal seed data is inserted so dbt models can build and
-- tests can run without requiring Spark or the full dataset.
-- ================================================================

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                      VARCHAR(50)  PRIMARY KEY,
    customer_id                   VARCHAR(50)  NOT NULL,
    order_status                  VARCHAR(20)  NOT NULL,
    order_purchase_timestamp      TIMESTAMP    NOT NULL,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id        VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_city      VARCHAR(100) NOT NULL,
    customer_state     VARCHAR(2)  NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id            VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id      VARCHAR(50)      NOT NULL,
    order_item_id INTEGER          NOT NULL,
    product_id    VARCHAR(50)      NOT NULL,
    seller_id     VARCHAR(50)      NOT NULL,
    price         DOUBLE PRECISION NOT NULL,
    freight_value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

-- ================================================================
-- Seed data for CI — minimal rows that satisfy all dbt test
-- constraints (not_null, unique, accepted_values, relationships,
-- accepted_range, assert_no_orphan_order_items)
-- ================================================================

INSERT INTO raw.customers (customer_id, customer_unique_id, customer_city, customer_state) VALUES
    ('cust_001', 'uniq_001', 'sao paulo',     'SP'),
    ('cust_002', 'uniq_002', 'rio de janeiro', 'RJ'),
    ('cust_003', 'uniq_003', 'belo horizonte', 'MG')
ON CONFLICT DO NOTHING;

INSERT INTO raw.products (product_id, product_category_name) VALUES
    ('prod_001', 'electronics'),
    ('prod_002', 'furniture'),
    ('prod_003', 'toys')
ON CONFLICT DO NOTHING;

INSERT INTO raw.orders (order_id, customer_id, order_status, order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date) VALUES
    ('ord_001', 'cust_001', 'delivered',   '2024-01-15 10:30:00', '2024-01-20 14:00:00', '2024-01-22 00:00:00'),
    ('ord_002', 'cust_002', 'shipped',     '2024-01-16 11:00:00', NULL,                  '2024-01-25 00:00:00'),
    ('ord_003', 'cust_003', 'processing',  '2024-01-17 09:15:00', NULL,                  '2024-01-28 00:00:00')
ON CONFLICT DO NOTHING;

INSERT INTO raw.order_items (order_id, order_item_id, product_id, seller_id, price, freight_value) VALUES
    ('ord_001', 1, 'prod_001', 'seller_001', 199.90, 15.50),
    ('ord_001', 2, 'prod_002', 'seller_002', 349.00, 25.00),
    ('ord_002', 1, 'prod_003', 'seller_001',  59.90,  8.75),
    ('ord_003', 1, 'prod_001', 'seller_003', 199.90, 12.00)
ON CONFLICT DO NOTHING;

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

-- Seed streaming data for CI
INSERT INTO streaming.raw_orders_stream (order_id, customer_id, product_id, amount, quantity, total_value, event_time, processing_time) VALUES
    ('stream_001', 'cust_001', 'prod_001', 99.90, 2, 199.80, '2024-01-15 10:30:00', '2024-01-15 10:30:05'),
    ('stream_002', 'cust_002', 'prod_002', 49.50, 1,  49.50, '2024-01-15 11:00:00', '2024-01-15 11:00:03')
ON CONFLICT DO NOTHING;

-- Indexes for analytical queries
CREATE INDEX IF NOT EXISTS idx_stream_event_time
    ON streaming.raw_orders_stream (event_time);

CREATE INDEX IF NOT EXISTS idx_stream_customer_id
    ON streaming.raw_orders_stream (customer_id);

CREATE INDEX IF NOT EXISTS idx_stream_product_id
    ON streaming.raw_orders_stream (product_id);
