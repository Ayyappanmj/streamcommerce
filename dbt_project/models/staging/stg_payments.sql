select payment_id, order_id, amount, method, payment_ts, payment_status
from {{ source('raw', 'payments') }}
