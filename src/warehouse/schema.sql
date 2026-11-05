-- Dimensional model (DuckDB). Executed by load_warehouse.py after the dw tables are dropped (full refresh
-- from the processed layer, which makes every load idempotent). Dimensions first so foreign keys resolve.
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE dw.dim_date (
    date_key INTEGER PRIMARY KEY,           -- YYYYMMDD
    full_date DATE NOT NULL,
    year INTEGER, quarter INTEGER, month INTEGER, month_name VARCHAR, day INTEGER,
    day_of_week INTEGER,                    -- 1 = Monday
    is_weekend BOOLEAN
);

CREATE TABLE dw.dim_customers (
    customer_key BIGINT PRIMARY KEY,
    customer_id VARCHAR NOT NULL UNIQUE,
    email VARCHAR, country VARCHAR, segment VARCHAR, signup_ts TIMESTAMP
);

CREATE TABLE dw.dim_products (
    product_key BIGINT PRIMARY KEY,
    product_id VARCHAR NOT NULL UNIQUE,
    product_name VARCHAR, category VARCHAR, unit_price DOUBLE CHECK (unit_price >= 0), created_ts TIMESTAMP
);

-- GRAIN: one row per order
CREATE TABLE dw.fact_orders (
    order_key BIGINT PRIMARY KEY,
    order_id VARCHAR NOT NULL UNIQUE,
    customer_key BIGINT NOT NULL REFERENCES dw.dim_customers (customer_key),
    order_date_key INTEGER NOT NULL REFERENCES dw.dim_date (date_key),
    order_ts TIMESTAMP, status VARCHAR, currency VARCHAR,
    order_total DOUBLE CHECK (order_total >= 0),
    item_count INTEGER, units INTEGER
);

-- GRAIN: one row per order item (line)
CREATE TABLE dw.fact_order_items (
    order_item_key BIGINT PRIMARY KEY,
    order_item_id VARCHAR NOT NULL UNIQUE,
    order_key BIGINT NOT NULL REFERENCES dw.fact_orders (order_key),
    product_key BIGINT NOT NULL REFERENCES dw.dim_products (product_key),
    order_date_key INTEGER NOT NULL REFERENCES dw.dim_date (date_key),
    quantity INTEGER CHECK (quantity > 0), unit_price DOUBLE, line_total DOUBLE
);

-- GRAIN: one row per payment transaction
CREATE TABLE dw.fact_payments (
    payment_key BIGINT PRIMARY KEY,
    payment_id VARCHAR NOT NULL UNIQUE,
    order_key BIGINT NOT NULL REFERENCES dw.fact_orders (order_key),
    payment_date_key INTEGER NOT NULL REFERENCES dw.dim_date (date_key),
    amount DOUBLE CHECK (amount >= 0), method VARCHAR, payment_status VARCHAR, payment_ts TIMESTAMP
);

-- GRAIN: one row per refund transaction
CREATE TABLE dw.fact_refunds (
    refund_key BIGINT PRIMARY KEY,
    refund_id VARCHAR NOT NULL UNIQUE,
    order_key BIGINT NOT NULL REFERENCES dw.fact_orders (order_key),
    refund_date_key INTEGER NOT NULL REFERENCES dw.dim_date (date_key),
    amount DOUBLE CHECK (amount >= 0), reason VARCHAR, refund_ts TIMESTAMP
);

CREATE INDEX idx_fact_orders_customer ON dw.fact_orders (customer_key);
CREATE INDEX idx_fact_items_product ON dw.fact_order_items (product_key);
CREATE INDEX idx_fact_items_order ON dw.fact_order_items (order_key);
