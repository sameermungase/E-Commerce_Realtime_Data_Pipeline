-- dim_products.sql
-- Dimension table: unique products with category

select distinct
    product_id,
    product_category
from {{ ref('stg_products') }}
