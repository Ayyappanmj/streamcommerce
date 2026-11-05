with refunds as (
    select order_id, sum(amount) as refunded from {{ ref('stg_refunds') }} group by order_id
)
select o.customer_id,
       count(*) as orders,
       sum(o.order_total) as total_spent,
       coalesce(sum(r.refunded), 0) as total_refunded,
       sum(o.order_total) - coalesce(sum(r.refunded), 0) as net_value,
       min(o.order_date) as first_order_date,
       max(o.order_date) as last_order_date
from {{ ref('stg_orders') }} o
left join refunds r on r.order_id = o.order_id
where o.status <> 'cancelled'
group by o.customer_id
