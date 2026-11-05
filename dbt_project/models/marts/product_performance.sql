select product_id, product_name, category,
       sum(quantity) as units_sold,
       sum(line_total) as revenue,
       count(distinct order_id) as orders
from {{ ref('int_order_items_clean') }}
group by product_id, product_name, category
