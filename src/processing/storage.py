"""Read/write the processed layer (Parquet by default, CSV for dependency-free mode) with atomic replace."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from src.common.config import Settings
from src.common.exceptions import MissingDependencyError
from src.common.schemas import DATASETS
from src.processing.transformations import coerce_types


def table_path(settings: Settings, name: str) -> Path:
    return settings.processed_dir / f"{name}.{settings.processed_format}"


def write_table(settings: Settings, name: str, df: pd.DataFrame) -> Path:
    """Write a processed table atomically (temp file + rename)."""
    path = table_path(settings, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if settings.processed_format == "parquet":
        try:
            df.to_parquet(tmp, index=False)
        except ImportError as exc:
            raise MissingDependencyError(
                "pyarrow is required for PROCESSED_FORMAT=parquet (pip install pyarrow)") from exc
    else:
        df.to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path


def read_table(settings: Settings, name: str) -> pd.DataFrame | None:
    """Return the processed table, or None if it does not exist yet."""
    path = table_path(settings, name)
    if not path.is_file():
        return None
    if settings.processed_format == "parquet":
        return pd.read_parquet(path)
    raw = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    return coerce_types(raw, DATASETS[name]).drop(columns=["_parse_errors"])


def upsert_table(settings: Settings, name: str, new: pd.DataFrame, key: str) -> pd.DataFrame:
    """Merge ``new`` into the stored table; the newest row per key wins. Re-running with the same input is a no-op."""
    existing = read_table(settings, name)
    merged = new if existing is None else pd.concat([existing, new], ignore_index=True)
    merged = merged.drop_duplicates(subset=[key], keep="last").sort_values(key).reset_index(drop=True)
    write_table(settings, name, merged)
    return merged
