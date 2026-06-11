-- dim_customers.sql
-- Dimension table: unique customers with city and state

select distinct
    customer_id,
    customer_city,
    customer_state
from {{ ref('stg_customers') }}
