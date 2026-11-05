-- Singular test: total of daily_sales must equal the order-grain total (no revenue multiplication).
select 1
from (
    select (select coalesce(sum(gross_sales), 0) from {{ ref('daily_sales') }}) as mart_total,
           (select coalesce(sum(order_total), 0) from {{ ref('stg_orders') }} where status <> 'cancelled') as order_total
) t
where abs(mart_total - order_total) > 0.01
