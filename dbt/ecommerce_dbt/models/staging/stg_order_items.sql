-- stg_order_items.sql
-- Staged order items: line-level detail with pricing

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value
from {{ source('raw', 'order_items') }}
where order_id is not null
  and product_id is not null
