-- fact_orders.sql
-- Fact table: joins orders with order_items to create
-- a line-item level fact table with pricing and timestamps.
--
-- Grain: one row per order line item (order_id + order_item_id)
-- PK:    fact_order_line_sk (surrogate, MD5 of order_id || order_item_id)

select
    {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.order_item_id']) }} as fact_order_line_sk,
    oi.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    o.purchase_timestamp
from {{ ref('stg_orders') }} as o
inner join {{ ref('stg_order_items') }} as oi
    on o.order_id = oi.order_id
