"""Data access for the dashboard. All values come from the real local warehouse / metadata store."""
from __future__ import annotations

import socket
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from src.common.config import Settings
from src.common.exceptions import MissingDependencyError, StreamCommerceError
from src.metadata.repository import get_repository
from src.processing.event_processor import EventStore
from src.warehouse.duckdb_client import connect


class DataUnavailable(Exception):
    """Raised when data cannot be loaded; the message is shown to the user."""


@st.cache_resource
def get_settings() -> Settings:
    return Settings.from_env()


@st.cache_data(ttl=15, show_spinner=False)
def query(sql: str) -> pd.DataFrame:
    """Run a read-only SQL query on the DuckDB warehouse."""
    settings = get_settings()
    if not settings.warehouse_path.exists():
        raise DataUnavailable("Warehouse not found. Run the batch pipeline first (see README).")
    try:
        con = connect(settings.warehouse_path, read_only=True)
    except MissingDependencyError as exc:
        raise DataUnavailable(str(exc)) from exc
    try:
        return con.execute(sql).df()
    except Exception as exc:  # duckdb raises several error types; surface the message, do not hide it
        raise DataUnavailable(f"Query failed: {exc}") from exc
    finally:
        con.close()


@st.cache_data(ttl=15, show_spinner=False)
def metadata(kind: str) -> pd.DataFrame:
    """Load a metadata table as a DataFrame: runs | quality | errors | datasets | manifests."""
    try:
        repo = get_repository(get_settings())
        rows = {"runs": lambda: repo.list_runs(200), "quality": repo.list_quality_results,
                "errors": repo.list_errors, "datasets": repo.list_dataset_metadata,
                "manifests": repo.list_manifests}[kind]()
    except StreamCommerceError as exc:
        raise DataUnavailable(str(exc)) from exc
    return pd.DataFrame(rows)


@st.cache_data(ttl=10, show_spinner=False)
def stream_tables() -> dict[str, pd.DataFrame]:
    """Streaming store tables as DataFrames (empty dict values when nothing has been consumed yet)."""
    settings = get_settings()
    if not settings.events_db_path.exists():
        raise DataUnavailable("No streaming data yet. Run the streaming demo (see README).")
    store = EventStore(settings.events_db_path)
    with store._connect() as conn:
        return {name: pd.read_sql_query(f"SELECT * FROM {name}", conn)  # fixed table names
                for name in ("processed_events", "quarantined_events", "duplicate_events")}


def tcp_reachable(host_port: str, timeout: float = 1.0) -> bool:
    host, _, port = host_port.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=timeout):
            return True
    except (OSError, ValueError):
        return False


def hours_since(ts: object) -> float | None:
    """Hours between now (UTC) and a timestamp; None if unparsable."""
    parsed = pd.to_datetime(ts, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return (datetime.now(timezone.utc) - parsed.to_pydatetime()).total_seconds() / 3600
