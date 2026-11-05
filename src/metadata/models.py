"""Metadata record types and the SQLite DDL used in lightweight mode.

The PostgreSQL DDL lives in ``sql/postgres_schema.sql`` (same tables, native types).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

STATUS_RUNNING, STATUS_SUCCESS, STATUS_FAILED = "running", "success", "failed"
MANIFEST_INGESTED, MANIFEST_VALIDATED, MANIFEST_PROCESSED, MANIFEST_FAILED = (
    "ingested", "validated", "processed", "failed")


def utc_now() -> str:
    """Current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class QualityResult:
    """Outcome of one rule on one dataset."""

    dataset_name: str
    rule_name: str
    severity: str
    total_records: int
    failed_records: int
    status: str  # passed | failed | skipped
    description: str
    checked_at: str


SQLITE_DDL = [
    """CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id TEXT PRIMARY KEY, pipeline_name TEXT NOT NULL, status TEXT NOT NULL,
        started_at TEXT NOT NULL, finished_at TEXT, records_processed INTEGER NOT NULL DEFAULT 0,
        records_rejected INTEGER NOT NULL DEFAULT 0, engine TEXT, details TEXT)""",
    """CREATE TABLE IF NOT EXISTS ingestion_manifests (
        source_file TEXT PRIMARY KEY, dataset TEXT NOT NULL, checksum TEXT NOT NULL,
        ingested_at TEXT NOT NULL, record_count INTEGER NOT NULL, status TEXT NOT NULL,
        error_details TEXT, landed_path TEXT, last_run_id TEXT)""",
    """CREATE TABLE IF NOT EXISTS data_quality_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, dataset_name TEXT NOT NULL,
        rule_name TEXT NOT NULL, severity TEXT NOT NULL, total_records INTEGER NOT NULL,
        failed_records INTEGER NOT NULL, status TEXT NOT NULL, description TEXT, checked_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS processing_errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, stage TEXT NOT NULL, dataset_name TEXT,
        error_type TEXT NOT NULL, message TEXT NOT NULL, occurred_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS dataset_metadata (
        dataset_name TEXT PRIMARY KEY, row_count INTEGER NOT NULL, last_updated_at TEXT NOT NULL,
        schema_columns TEXT, source TEXT)""",
]
