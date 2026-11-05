select order_item_id, order_id, product_id, quantity, unit_price, line_total
from {{ source('raw', 'order_items') }}
