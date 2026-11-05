"""Load the processed layer into DuckDB.

* ``raw.*``  - cleaned datasets, streaming events, pipeline metadata (inputs for dbt);
* ``dw.*``   - dimensional model built in pandas and loaded with enforced keys.

The whole load runs in ONE transaction and rebuilds tables from the processed layer, so running it twice on
the same input yields identical tables (idempotent) and a failure leaves the previous warehouse untouched.
Table names below come from fixed internal lists, never from user input.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.common.config import Settings
from src.common.exceptions import IngestionError
from src.common.schemas import DATASET_ORDER
from src.metadata.repository import MetadataRepository
from src.processing.event_processor import EventStore
from src.processing.storage import read_table
from src.processing.transformations import build_warehouse_tables, to_naive_utc
from src.warehouse.duckdb_client import connect, read_sql_file

log = logging.getLogger(__name__)
SCHEMA_SQL = Path(__file__).with_name("schema.sql")
DW_LOAD_ORDER = ["dim_date", "dim_customers", "dim_products", "fact_orders", "fact_order_items",
                 "fact_payments", "fact_refunds"]


def _replace(con, schema_table: str, df: pd.DataFrame) -> None:
    con.register("_stage_df", df)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {schema_table} AS SELECT * FROM _stage_df")
    finally:
        con.unregister("_stage_df")


def _events_frame(settings: Settings) -> pd.DataFrame:
    cols = ["event_id", "event_type", "event_ts", "ingested_at", "order_id", "customer_id", "order_total",
            "refund_amount", "items_json", "is_late", "source"]
    if not settings.events_db_path.exists():
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(EventStore(settings.events_db_path).fetch_events(), columns=cols)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True).dt.tz_localize(None)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True, format="ISO8601").dt.tz_localize(None)
    df["is_late"] = df["is_late"].astype(bool)
    return df


def _metadata_frames(repo: MetadataRepository | None) -> dict[str, pd.DataFrame]:
    q_cols = ["id", "run_id", "dataset_name", "rule_name", "severity", "total_records", "failed_records", "status",
              "description", "checked_at"]
    quality = pd.DataFrame(repo.list_quality_results() if repo else [], columns=q_cols)
    if not quality.empty:
        quality["checked_at"] = pd.to_datetime(quality["checked_at"], utc=True, format="ISO8601").dt.tz_localize(None)
    return {"quality_results": quality}


def load_warehouse(settings: Settings, repo: MetadataRepository | None = None) -> dict[str, int]:
    """Rebuild the DuckDB warehouse from processed data. Returns row counts per table."""
    frames = {name: read_table(settings, name) for name in DATASET_ORDER}
    missing = [n for n, f in frames.items() if f is None]
    if missing:
        raise IngestionError(f"Processed datasets missing: {missing}. Run the batch pipeline first.")
    tables = {k: to_naive_utc(v) for k, v in build_warehouse_tables(frames).items()}
    raw = {k: to_naive_utc(v) for k, v in frames.items()}
    raw["order_events"] = _events_frame(settings)
    raw.update(_metadata_frames(repo))

    con = connect(settings.warehouse_path)
    counts: dict[str, int] = {}
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        for name, df in raw.items():
            _replace(con, f"raw.{name}", df)
            counts[f"raw.{name}"] = len(df)
        for name in reversed(DW_LOAD_ORDER):
            con.execute(f"DROP TABLE IF EXISTS dw.{name}")
        con.execute(read_sql_file(SCHEMA_SQL))
        for name in DW_LOAD_ORDER:
            df = tables[name]
            con.register("_stage_df", df)
            cols = ", ".join(df.columns)
            con.execute(f"INSERT INTO dw.{name} ({cols}) SELECT {cols} FROM _stage_df")
            con.unregister("_stage_df")
            counts[f"dw.{name}"] = len(df)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    log.info("warehouse loaded", extra={"tables": counts})
    return counts
