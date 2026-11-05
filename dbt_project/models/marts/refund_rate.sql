-- Grain: one row per order month. Refunds are pre-aggregated per order before joining.
with refunds as (
    select order_id, sum(amount) as refunded from {{ ref('stg_refunds') }} group by order_id
)
select date_trunc('month', o.order_date) as order_month,
       count(*) as orders,
       sum(case when r.order_id is not null then 1 else 0 end) as refunded_orders,
       sum(o.order_total) as gross_sales,
       coalesce(sum(r.refunded), 0) as refund_amount,
       case when count(*) = 0 then null
            else sum(case when r.order_id is not null then 1 else 0 end) * 1.0 / count(*) end as refund_rate
from {{ ref('stg_orders') }} o
left join refunds r on r.order_id = o.order_id
where o.status <> 'cancelled'
group by 1
