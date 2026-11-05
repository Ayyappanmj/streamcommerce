select product_id, product_name, category, unit_price, created_ts
from {{ source('raw', 'products') }}
