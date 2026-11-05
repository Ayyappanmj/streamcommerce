-- PostgreSQL metadata schema. Idempotent: safe to run repeatedly, never drops anything.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    pipeline_name     TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    records_processed INTEGER NOT NULL DEFAULT 0,
    records_rejected  INTEGER NOT NULL DEFAULT 0,
    engine            TEXT,
    details           TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_manifests (
    source_file   TEXT PRIMARY KEY,
    dataset       TEXT NOT NULL,
    checksum      TEXT NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL,
    record_count  INTEGER NOT NULL,
    status        TEXT NOT NULL,
    error_details TEXT,
    landed_path   TEXT,
    last_run_id   TEXT
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    id             BIGSERIAL PRIMARY KEY,
    run_id         TEXT NOT NULL,
    dataset_name   TEXT NOT NULL,
    rule_name      TEXT NOT NULL,
    severity       TEXT NOT NULL,
    total_records  INTEGER NOT NULL,
    failed_records INTEGER NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('passed', 'failed', 'skipped')),
    description    TEXT,
    checked_at     TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dq_run ON data_quality_results (run_id);

CREATE TABLE IF NOT EXISTS processing_errors (
    id           BIGSERIAL PRIMARY KEY,
    run_id       TEXT,
    stage        TEXT NOT NULL,
    dataset_name TEXT,
    error_type   TEXT NOT NULL,
    message      TEXT NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_err_run ON processing_errors (run_id);

CREATE TABLE IF NOT EXISTS dataset_metadata (
    dataset_name    TEXT PRIMARY KEY,
    row_count       INTEGER NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL,
    schema_columns  TEXT,
    source          TEXT
);

-- Operational reference data (loaded by src/ingestion/postgres_ingestion.py)
CREATE TABLE IF NOT EXISTS ref_customers (
    customer_id TEXT PRIMARY KEY, email TEXT NOT NULL, country TEXT, segment TEXT, signup_ts TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS ref_products (
    product_id TEXT PRIMARY KEY, product_name TEXT NOT NULL, category TEXT,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0), created_ts TIMESTAMPTZ
);
