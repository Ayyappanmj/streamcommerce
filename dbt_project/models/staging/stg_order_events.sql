-- Events are already unique per event_id (idempotent store); defensive de-duplication keeps the model safe.
select * exclude (rn) from (
    select event_id, event_type, event_ts, ingested_at, order_id, customer_id, order_total, refund_amount,
           is_late, row_number() over (partition by event_id order by ingested_at) as rn
    from {{ source('raw', 'order_events') }}
) where rn = 1
