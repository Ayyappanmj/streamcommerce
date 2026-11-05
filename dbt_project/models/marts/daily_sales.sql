-- Grain: one row per day. Revenue comes from the order grain; units are pre-aggregated per order,
-- so joining items can never multiply order_total.
with units as (
    select order_id, sum(quantity) as units from {{ ref('stg_order_items') }} group by order_id
)
select o.order_date,
       count(*) as orders,
       sum(o.order_total) as gross_sales,
       sum(o.order_total) / count(*) as avg_order_value,
       coalesce(sum(u.units), 0) as units_sold
from {{ ref('stg_orders') }} o
left join units u on u.order_id = o.order_id
where o.status <> 'cancelled'
group by o.order_date
