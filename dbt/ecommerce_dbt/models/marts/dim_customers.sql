-- dim_customers.sql
-- Dimension table: unique customers with city and state.
--
-- Deduplication: uses ROW_NUMBER() ordered by customer_id (deterministic)
-- instead of SELECT DISTINCT to guarantee one row per customer_id
-- and produce a predictable result when customer data is re-loaded.

with ranked as (
    select
        customer_id,
        customer_city,
        customer_state,
        row_number() over (
            partition by customer_id
            order by customer_id
        ) as rn
    from {{ ref('stg_customers') }}
)

select
    customer_id,
    customer_city,
    customer_state
from ranked
where rn = 1
