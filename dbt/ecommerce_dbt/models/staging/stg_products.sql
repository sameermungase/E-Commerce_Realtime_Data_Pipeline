-- stg_products.sql
-- Staged products: cleaned category names

select
    product_id,
    product_category_name as product_category
from {{ source('raw', 'products') }}
where product_id is not null
