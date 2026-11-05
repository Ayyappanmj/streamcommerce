with per_customer as (
    select customer_id, count(*) as orders from {{ ref('stg_orders') }} where status <> 'cancelled' group by customer_id
)
select count(*) as customers_with_orders,
       sum(case when orders >= 2 then 1 else 0 end) as repeat_customers,
       case when count(*) = 0 then null else sum(case when orders >= 2 then 1 else 0 end) * 1.0 / count(*) end
           as repeat_purchase_rate
from per_customer
