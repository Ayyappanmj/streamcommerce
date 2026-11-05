# Data dictionary

All identifiers are synthetic. Timestamps are UTC.

## Input CSV datasets (`data/raw/incoming/*.csv`)
| Dataset | Column | Type | Rules |
|---|---|---|---|
| customers | customer_id | str | required, `^C\d{6,}$`, unique |
| | email | str | required (synthetic `@example.com`) |
| | country, segment | str | optional |
| | signup_ts | timestamp | required, ISO-8601 |
| products | product_id | str | required, `^P\d{5,}$`, unique |
| | product_name, category | str | name required |
| | unit_price | float | required, >= 0 |
| | created_ts | timestamp | optional |
| orders | order_id | str | required, `^ORD-\d{7,}$`, unique |
| | customer_id | str | required, must exist in customers |
| | order_ts | timestamp | required |
| | status | str | placed, paid, shipped, delivered, cancelled |
| | currency | str | USD |
| | order_total | float | required, >= 0 |
| order_items | order_item_id | str | required, unique |
| | order_id, product_id | str | must exist in orders / products |
| | quantity | int | > 0 |
| | unit_price, line_total | float | >= 0 |
| payments | payment_id | str | required, unique |
| | order_id | str | must exist in orders |
| | amount | float | >= 0 (warn: sum should equal order_total) |
| | method, payment_status, payment_ts | | status: captured, failed |
| refunds | refund_id | str | required, unique |
| | order_id, amount, refund_ts, reason | | order must exist, amount >= 0 |

## Streaming event schema (JSON, `schema_version` = 1)
```json
{"event_id": "uuid", "event_type": "order_created", "event_ts": "2025-01-01T10:00:00Z", "schema_version": 1,
 "order_id": "ORD-0000001", "customer_id": "C000001", "order_total": 42.5, "currency": "USD",
 "items": [{"product_id": "P00001", "quantity": 2, "unit_price": 21.25}], "refund_amount": null}
```
`event_type` is one of `order_created, order_paid, order_shipped, order_cancelled, order_refunded`.
`order_created` requires `customer_id` and a non-empty `items` list (quantity > 0, unit_price >= 0);
`order_refunded` requires `refund_amount >= 0`. Anything else is quarantined with the reasons listed.

## Streaming store (`data/processed/events.sqlite`)
`processed_events` (PK `event_id`; `event_ts` business time, `ingested_at` pipeline time, `is_late`),
`quarantined_events` (raw payload + reasons), `duplicate_events` (every ignored redelivery).

## Metadata tables (SQLite `data/metadata.db` or PostgreSQL)
| Table | Purpose |
|---|---|
| pipeline_runs | run_id (PK), status, start/end time, records_processed, records_rejected, engine |
| ingestion_manifests | source_file (PK), checksum, record_count, status (ingested/validated/processed/failed), error_details |
| data_quality_results | run_id, dataset_name, rule_name, severity, total_records, failed_records, status, description, checked_at |
| processing_errors | run_id, stage, dataset_name, error_type, message |
| dataset_metadata | row count and columns per processed dataset |

## Warehouse (DuckDB `data/warehouse/streamcommerce.duckdb`)
* `raw.*` – cleaned datasets, `raw.order_events`, `raw.quality_results` (inputs for dbt).
* `dw.*` – dimensional model (see grain table in ARCHITECTURE.md): `dim_customers, dim_products, dim_date,
  fact_orders, fact_order_items, fact_payments, fact_refunds` with primary keys, foreign keys, CHECK constraints, indexes.
* `staging.*`, `analytics.*` – dbt output: `daily_sales, product_performance, customer_lifetime_value,
  repeat_purchase_rate, refund_rate, dq_summary, stream_sales`.

## Metric definitions
* **Gross sales** = sum of `order_total` for orders whose status is not `cancelled`.
* **Average order value** = gross sales / non-cancelled orders.
* **Returning customer %** = customers with >= 2 non-cancelled orders / customers with >= 1.
* **Refund rate (orders)** = orders with at least one refund / non-cancelled orders (dbt `refund_rate` also gives the amount).
