"""Shared test helpers (plain functions, no hidden fixtures)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.config import Settings
from src.generators.cli import generate_dataset
from src.metadata.repository import get_repository
from src.processing.storage import read_table


def make_settings(tmp_path: Path, **overrides: object) -> Settings:
    settings = Settings.for_data_dir(tmp_path / "data", **overrides)
    settings.ensure_dirs()
    return settings


def small_dataset(settings: Settings, faults: bool = False, orders: int = 200, seed: int = 7) -> dict:
    return generate_dataset(settings.incoming_dir, customers=40, products=15, orders=orders, seed=seed, faults=faults)


def run_batch(settings: Settings, run_id: str = "test-run", force: bool = False,
              steps: list[str] | None = None) -> dict:
    from src.batch_pipeline import run_pipeline
    steps = steps or ["prepare", "ingest", "validate", "process", "final_quality", "summary"]
    return run_pipeline(settings, steps, run_id=run_id, force=force)


def processed_snapshot(settings: Settings) -> dict[str, pd.DataFrame]:
    names = ["customers", "products", "orders", "order_items", "payments", "refunds"]
    return {n: read_table(settings, n).reset_index(drop=True) for n in names}


def repo_for(settings: Settings):
    return get_repository(settings)
