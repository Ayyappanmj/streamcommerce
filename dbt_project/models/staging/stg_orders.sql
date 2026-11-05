select order_id, customer_id, order_ts, cast(order_ts as date) as order_date, status, currency, order_total
from {{ source('raw', 'orders') }}
