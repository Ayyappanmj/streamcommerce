"""Thin DuckDB helpers (import is lazy so the rest of the project works without duckdb installed)."""
from __future__ import annotations

from pathlib import Path

from src.common.exceptions import MissingDependencyError


def connect(path: Path, read_only: bool = False):
    """Open the DuckDB warehouse file, creating its folder when writing."""
    try:
        import duckdb
    except ImportError as exc:
        raise MissingDependencyError("duckdb is not installed (pip install duckdb)") from exc
    if not read_only:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def read_sql_file(path: Path) -> str:
    return Path(path).read_text(encoding="utf-8")
