-- fact_orders_realtime.sql
-- Real-time fact table from the streaming pipeline.
--
-- Source: streaming.raw_orders_stream (populated by Spark Structured Streaming)
-- Contains individual order events with computed total_value,
-- plus derived date/hour columns for time-series aggregations.

select
    order_id,
    customer_id,
    product_id,
    amount,
    quantity,
    total_value,
    event_time,
    processing_time,
    date_trunc('day', event_time)::date  as order_date,
    date_trunc('hour', event_time)       as order_hour
from {{ source('streaming', 'raw_orders_stream') }}
where order_id is not null
