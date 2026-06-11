-- fact_orders.sql
-- Fact table: joins orders with order_items to create
-- a line-item level fact table with pricing and timestamps.

select
    oi.order_id,
    o.customer_id,
    oi.product_id,
    oi.price,
    oi.freight_value,
    o.purchase_timestamp
from {{ ref('stg_orders') }} o
inner join {{ ref('stg_order_items') }} oi
    on o.order_id = oi.order_id
