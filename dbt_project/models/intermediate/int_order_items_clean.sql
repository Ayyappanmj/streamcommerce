-- Valid line items of non-cancelled orders, enriched with product and order context.
select i.order_item_id, i.order_id, i.product_id, p.product_name, p.category, i.quantity, i.unit_price,
       i.line_total, o.order_date, o.customer_id
from {{ ref('stg_order_items') }} i
join {{ ref('stg_orders') }} o on o.order_id = i.order_id
join {{ ref('stg_products') }} p on p.product_id = i.product_id
where i.quantity > 0 and o.status <> 'cancelled'
