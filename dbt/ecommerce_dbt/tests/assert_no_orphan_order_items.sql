-- assert_no_orphan_order_items.sql
-- Custom singular test: ensures no order_items exist without a matching order.
--
-- This is a data integrity check that catches upstream ETL issues
-- where order_items might reference orders that were filtered out
-- during cleaning or failed to load.
--
-- Test passes when this query returns 0 rows.

select
    oi.order_id,
    oi.order_item_id,
    oi.product_id
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id
where o.order_id is null
