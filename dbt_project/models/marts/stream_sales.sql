-- Streaming-derived daily sales: each order counted once (earliest order_created event), so replays and
-- duplicate deliveries cannot inflate totals.
with first_created as (
    select order_id, order_total, event_ts,
           row_number() over (partition by order_id order by event_ts, event_id) as rn
    from {{ ref('stg_order_events') }}
    where event_type = 'order_created'
)
select cast(event_ts as date) as event_date, count(*) as orders, sum(order_total) as gross_sales
from first_created where rn = 1
group by 1
