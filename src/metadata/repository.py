"""Metadata repository: pipeline runs, manifests, quality results, errors, dataset metadata.

Two backends share one code path:
* ``sqlite``   - default for lightweight local mode (standard library only);
* ``postgres`` - selected with METADATA_BACKEND=postgres (psycopg 3).

All statements are parameterised (``?`` placeholders, translated to ``%s`` for PostgreSQL)
and every write happens inside a transaction that is rolled back on error.
"""
from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT, Settings
from src.common.exceptions import MetadataError, MissingDependencyError
from src.metadata.models import SQLITE_DDL, QualityResult, utc_now

log = logging.getLogger(__name__)


class MetadataRepository:
    """Thin data-access layer over the metadata store."""

    def __init__(self, settings: Settings) -> None:
        self._backend = settings.metadata_backend
        self._sqlite_path = settings.metadata_sqlite_path
        self._dsn = settings.postgres_dsn if self._backend == "postgres" else ""

    # ---- connection handling ----------------------------------------------
    @contextmanager
    def _cursor(self) -> Iterator[Any]:
        if self._backend == "sqlite":
            self._sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self._sqlite_path, timeout=30)
            conn.row_factory = sqlite3.Row
        else:
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError as exc:  # pragma: no cover - depends on environment
                raise MissingDependencyError("psycopg is required for METADATA_BACKEND=postgres") from exc
            try:
                conn = psycopg.connect(self._dsn, row_factory=dict_row, connect_timeout=5)
            except psycopg.OperationalError as exc:
                raise MetadataError(
                    "Cannot connect to PostgreSQL. Start it (docker compose up -d postgres) or set "
                    "METADATA_BACKEND=sqlite for lightweight local mode.") from exc
        try:
            cur = conn.cursor()
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _run(self, cur: Any, sql: str, params: tuple = ()) -> Any:
        if self._backend == "postgres":
            sql = sql.replace("?", "%s")
        cur.execute(sql, params)
        return cur

    def _fetch(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._cursor() as cur:
            self._run(cur, sql, params)
            return [dict(r) for r in cur.fetchall()]

    # ---- schema --------------------------------------------------------------
    def init_schema(self) -> None:
        """Create metadata tables if they do not exist (never drops anything)."""
        with self._cursor() as cur:
            if self._backend == "sqlite":
                for ddl in SQLITE_DDL:
                    cur.execute(ddl)
            else:
                cur.execute((PROJECT_ROOT / "sql" / "postgres_schema.sql").read_text(encoding="utf-8"))

    # ---- pipeline runs -------------------------------------------------------
    def start_run(self, run_id: str, pipeline_name: str, engine: str) -> None:
        """Register a run. Re-registering an existing run_id is a no-op (safe for Airflow retries)."""
        with self._cursor() as cur:
            self._run(cur, "INSERT INTO pipeline_runs (run_id, pipeline_name, status, started_at, engine) "
                           "VALUES (?, ?, 'running', ?, ?) ON CONFLICT (run_id) DO NOTHING",
                      (run_id, pipeline_name, utc_now(), engine))

    def add_run_counts(self, run_id: str, processed: int, rejected: int) -> None:
        with self._cursor() as cur:
            self._run(cur, "UPDATE pipeline_runs SET records_processed = records_processed + ?, "
                           "records_rejected = records_rejected + ? WHERE run_id = ?",
                      (processed, rejected, run_id))

    def finish_run(self, run_id: str, status: str, details: str | None = None) -> None:
        with self._cursor() as cur:
            self._run(cur, "UPDATE pipeline_runs SET status = ?, finished_at = ?, details = ? WHERE run_id = ?",
                      (status, utc_now(), details, run_id))

    def get_run(self, run_id: str) -> dict | None:
        rows = self._fetch("SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,))
        return rows[0] if rows else None

    def list_runs(self, limit: int = 50) -> list[dict]:
        return self._fetch("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?", (limit,))

    # ---- manifests -------------------------------------------------------------
    def get_manifest(self, source_file: str) -> dict | None:
        rows = self._fetch("SELECT * FROM ingestion_manifests WHERE source_file = ?", (source_file,))
        return rows[0] if rows else None

    def list_manifests(self, status: str | None = None) -> list[dict]:
        if status:
            return self._fetch("SELECT * FROM ingestion_manifests WHERE status = ? ORDER BY source_file", (status,))
        return self._fetch("SELECT * FROM ingestion_manifests ORDER BY source_file")

    def upsert_manifest(self, source_file: str, dataset: str, checksum: str, record_count: int, status: str,
                        landed_path: str | None, run_id: str | None, error: str | None = None) -> None:
        with self._cursor() as cur:
            self._run(cur,
                      "INSERT INTO ingestion_manifests (source_file, dataset, checksum, ingested_at, record_count, "
                      "status, error_details, landed_path, last_run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                      "ON CONFLICT (source_file) DO UPDATE SET dataset = excluded.dataset, checksum = excluded.checksum, "
                      "ingested_at = excluded.ingested_at, record_count = excluded.record_count, "
                      "status = excluded.status, error_details = excluded.error_details, "
                      "landed_path = excluded.landed_path, last_run_id = excluded.last_run_id",
                      (source_file, dataset, checksum, utc_now(), record_count, status, error, landed_path, run_id))

    def set_manifest_status(self, source_file: str, status: str, error: str | None = None) -> None:
        with self._cursor() as cur:
            self._run(cur, "UPDATE ingestion_manifests SET status = ?, error_details = ? WHERE source_file = ?",
                      (status, error, source_file))

    # ---- quality results / errors / dataset metadata -----------------------------
    def record_quality_results(self, run_id: str, results: list[QualityResult]) -> None:
        with self._cursor() as cur:
            for r in results:
                self._run(cur, "INSERT INTO data_quality_results (run_id, dataset_name, rule_name, severity, "
                               "total_records, failed_records, status, description, checked_at) "
                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (run_id, r.dataset_name, r.rule_name, r.severity, r.total_records, r.failed_records,
                           r.status, r.description, r.checked_at))

    def list_quality_results(self, run_id: str | None = None) -> list[dict]:
        if run_id:
            return self._fetch("SELECT * FROM data_quality_results WHERE run_id = ? ORDER BY id", (run_id,))
        return self._fetch("SELECT * FROM data_quality_results ORDER BY id")

    def record_error(self, run_id: str | None, stage: str, dataset: str | None, error_type: str, message: str) -> None:
        with self._cursor() as cur:
            self._run(cur, "INSERT INTO processing_errors (run_id, stage, dataset_name, error_type, message, "
                           "occurred_at) VALUES (?, ?, ?, ?, ?, ?)",
                      (run_id, stage, dataset, error_type, message[:2000], utc_now()))

    def list_errors(self, run_id: str | None = None) -> list[dict]:
        if run_id:
            return self._fetch("SELECT * FROM processing_errors WHERE run_id = ? ORDER BY id", (run_id,))
        return self._fetch("SELECT * FROM processing_errors ORDER BY id DESC LIMIT 500")

    def upsert_dataset_metadata(self, dataset: str, row_count: int, columns: list[str], source: str) -> None:
        with self._cursor() as cur:
            self._run(cur, "INSERT INTO dataset_metadata (dataset_name, row_count, last_updated_at, schema_columns, "
                           "source) VALUES (?, ?, ?, ?, ?) ON CONFLICT (dataset_name) DO UPDATE SET "
                           "row_count = excluded.row_count, last_updated_at = excluded.last_updated_at, "
                           "schema_columns = excluded.schema_columns, source = excluded.source",
                      (dataset, row_count, utc_now(), ",".join(columns), source))

    def list_dataset_metadata(self) -> list[dict]:
        return self._fetch("SELECT * FROM dataset_metadata ORDER BY dataset_name")


def get_repository(settings: Settings, init: bool = True) -> MetadataRepository:
    """Build a repository and (by default) make sure the schema exists."""
    repo = MetadataRepository(settings)
    if init:
        repo.init_schema()
    return repo


def sqlite_path_for(settings: Settings) -> Path:
    return settings.metadata_sqlite_path
