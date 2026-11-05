select refund_id, order_id, amount, refund_ts, cast(refund_ts as date) as refund_date, reason
from {{ source('raw', 'refunds') }}
