-- stg_customers.sql
-- Staged customers: deduplicated with clean location fields

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
from {{ source('raw', 'customers') }}
where customer_id is not null
