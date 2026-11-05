"""Opt-in loader: curated DuckDB tables -> Snowflake (stage via write_pandas, then MERGE).

Safety: no DROP / CREATE OR REPLACE on target tables. Target tables are created with IF NOT EXISTS.
Only the ``<TABLE>_STG`` staging table is overwritten each run. Use ``--dry-run`` to print the plan.

    python -m src.snowflake.loader --dry-run
    python -m src.snowflake.loader
"""
from __future__ import annotations

import argparse
import logging
import re
import sys

import pandas as pd

from src.common.config import Settings
from src.common.exceptions import StreamCommerceError
from src.common.logging_config import configure_logging
from src.snowflake.client import connect, load_config
from src.warehouse.duckdb_client import connect as duckdb_connect

log = logging.getLogger(__name__)
IDENT = re.compile(r"^[A-Z][A-Z0-9_]*$")
TABLES = {"DIM_CUSTOMERS": "CUSTOMER_KEY", "DIM_PRODUCTS": "PRODUCT_KEY", "DIM_DATE": "DATE_KEY",
          "FACT_ORDERS": "ORDER_KEY", "FACT_ORDER_ITEMS": "ORDER_ITEM_KEY",
          "FACT_PAYMENTS": "PAYMENT_KEY", "FACT_REFUNDS": "REFUND_KEY"}
SF_TYPES = {"int64": "NUMBER(38,0)", "Int64": "NUMBER(38,0)", "float64": "FLOAT", "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP_NTZ", "datetime64[us]": "TIMESTAMP_NTZ"}


def _ident(name: str) -> str:
    if not IDENT.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def build_merge_sql(table: str, columns: list[str], key: str) -> str:
    """MERGE staging into target. Identifiers are validated; values never appear in SQL text."""
    table, key = _ident(table), _ident(key)
    cols = [_ident(c) for c in columns]
    sets = ", ".join(f"t.{c} = s.{c}" for c in cols if c != key)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)
    return (f"MERGE INTO {table} t USING {table}_STG s ON t.{key} = s.{key} "
            f"WHEN MATCHED THEN UPDATE SET {sets} WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})")


def build_create_sql(table: str, df: pd.DataFrame) -> str:
    cols = ", ".join(f"{_ident(c.upper())} {SF_TYPES.get(str(t), 'VARCHAR')}" for c, t in df.dtypes.items())
    return f"CREATE TABLE IF NOT EXISTS {_ident(table)} ({cols})"


def load(settings: Settings, dry_run: bool = False) -> dict[str, int]:
    """Load all warehouse tables. With ``dry_run`` nothing is sent to Snowflake."""
    duck = duckdb_connect(settings.warehouse_path, read_only=True)
    try:
        frames = {t: duck.execute(f"SELECT * FROM dw.{t.lower()}").df() for t in TABLES}  # fixed table names
    finally:
        duck.close()
    for df in frames.values():
        df.columns = [c.upper() for c in df.columns]
    if dry_run:
        for t, df in frames.items():
            print(f"[dry-run] {t}: {len(df)} rows -> {build_create_sql(t, df)}")
            print(f"[dry-run] {build_merge_sql(t, list(df.columns), TABLES[t])}")
        return {t: len(df) for t, df in frames.items()}

    from snowflake.connector.pandas_tools import write_pandas  # imported lazily: optional dependency
    conn = connect(load_config())
    try:
        cur = conn.cursor()
        for t, df in frames.items():
            cur.execute(build_create_sql(t, df))
            write_pandas(conn, df, f"{t}_STG", auto_create_table=True, overwrite=True)
            cur.execute(build_merge_sql(t, list(df.columns), TABLES[t]))
            log.info("snowflake table merged", extra={"table": t, "rows": len(df)})
    finally:
        conn.close()
    return {t: len(df) for t, df in frames.items()}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Optional Snowflake loader")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    try:
        counts = load(settings, a.dry_run)
    except StreamCommerceError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
