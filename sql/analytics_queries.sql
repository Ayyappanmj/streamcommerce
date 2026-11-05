-- Example analytical queries for the DuckDB warehouse (open with: duckdb data/warehouse/streamcommerce.duckdb)

-- Gross sales (order grain: never join items before summing order_total)
SELECT count(*) AS orders, sum(order_total) AS gross_sales
FROM dw.fact_orders WHERE status <> 'cancelled';

-- Revenue by category (line-item grain)
SELECT p.category, sum(i.line_total) AS revenue
FROM dw.fact_order_items i
JOIN dw.dim_products p USING (product_key)
JOIN dw.fact_orders o USING (order_key)
WHERE o.status <> 'cancelled'
GROUP BY 1 ORDER BY 2 DESC;

-- Monthly refund rate (refunds pre-aggregated per order)
WITH r AS (SELECT order_key, sum(amount) AS refunded FROM dw.fact_refunds GROUP BY 1)
SELECT date_trunc('month', o.order_ts) AS month, count(*) AS orders,
       count(r.order_key) AS refunded_orders, sum(r.refunded) AS refund_amount
FROM dw.fact_orders o LEFT JOIN r USING (order_key)
WHERE o.status <> 'cancelled' GROUP BY 1 ORDER BY 1;

-- Orders whose payments do not add up to the order total
SELECT o.order_id, o.order_total, sum(p.amount) AS paid
FROM dw.fact_orders o JOIN dw.fact_payments p USING (order_key)
GROUP BY 1, 2 HAVING abs(o.order_total - sum(p.amount)) > 0.01;

-- Streaming vs batch reconciliation (streaming counts each order once)
SELECT (SELECT sum(order_total) FROM (SELECT order_id, any_value(order_total) AS order_total
        FROM raw.order_events WHERE event_type = 'order_created' GROUP BY order_id)) AS stream_sales;
