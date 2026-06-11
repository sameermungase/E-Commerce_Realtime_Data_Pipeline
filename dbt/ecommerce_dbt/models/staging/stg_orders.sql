-- stg_orders.sql
-- Staged orders: select relevant columns with clean types

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp as purchase_timestamp,
    order_delivered_customer_date as delivered_timestamp,
    order_estimated_delivery_date as estimated_delivery_timestamp
from {{ source('raw', 'orders') }}
where order_id is not null
