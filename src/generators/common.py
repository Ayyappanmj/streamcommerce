"""Helpers shared by the synthetic data generators."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_START_DATE = "2025-01-01"


def iso(ts: datetime) -> str:
    """Format a datetime as UTC ISO-8601 with a trailing Z."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_start_date(value: str) -> datetime:
    """Parse YYYY-MM-DD into a UTC midnight datetime."""
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def write_csv(path: Path, columns: list[str], rows: list[dict], extra_raw_lines: list[str] | None = None) -> None:
    """Write rows as CSV; ``extra_raw_lines`` are appended verbatim (used for malformed-record faults)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        for line in extra_raw_lines or []:
            handle.write(line + "\r\n")
