-- fact_orders_realtime.sql
-- Real-time fact table from the streaming pipeline.
--
-- Source: streaming.raw_orders_stream (populated by Spark Structured Streaming)
-- Contains individual order events with computed total_value,
-- plus derived date/hour columns for time-series aggregations.
--
-- Materialization: incremental (append new events only)
-- Unique key:      order_id (matches PRIMARY KEY in raw_orders_stream)
-- Strategy:        append_new_rows — only adds rows whose order_id
--                  doesn't already exist in the target table.
--
-- This avoids a full table rebuild on every dbt run, which would
-- be expensive at streaming scale (millions of rows per day).

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='append',
        on_schema_change='append_new_columns'
    )
}}

select
    order_id,
    customer_id,
    product_id,
    amount,
    quantity,
    total_value,
    event_time,
    processing_time,
    cast(date_trunc('day', event_time) as date) as order_date,
    date_trunc('hour', event_time) as order_hour
from {{ source('streaming', 'raw_orders_stream') }}
where
    order_id is not null

    {% if is_incremental() %}
        -- On incremental runs, only process events newer than the latest
        -- already in the target table
        and event_time > (select max(target.event_time) from {{ this }} as target)
    {% endif %}
