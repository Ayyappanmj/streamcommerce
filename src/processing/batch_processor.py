"""Batch processing: clean, deduplicate, validate, quarantine and persist each validated file."""
from __future__ import annotations

import csv
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from src.common.config import Settings
from src.common.schemas import DATASET_ORDER, DATASETS
from src.ingestion.csv_ingestion import read_raw_csv
from src.metadata.models import MANIFEST_FAILED, MANIFEST_PROCESSED, MANIFEST_VALIDATED
from src.metadata.repository import MetadataRepository
from src.processing.deduplication import split_duplicates
from src.processing.storage import read_table, upsert_table
from src.processing.transformations import coerce_types
from src.quality.quality_rules import load_rules
from src.quality.quality_runner import run_rules

log = logging.getLogger(__name__)
REFERENCES = {"orders": ["customers"], "order_items": ["orders", "products"],
              "payments": ["orders"], "refunds": ["orders"]}


@dataclass
class DatasetResult:
    dataset: str
    records_read: int
    valid: int
    rejected: int
    duplicates: int
    malformed: int
    quarantine_file: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)


def scan_malformed(path: Path, ncols: int) -> list[str]:
    """Return raw lines whose field count differs from the header (used by the Spark engine)."""
    bad = []
    with path.open("r", newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        bad = [",".join(row) for row in reader if row and len(row) != ncols]
    return bad


def _write_quarantine(settings: Settings, dataset: str, run_id: str, checksum: str, source: str,
                      frames: list[pd.DataFrame]) -> str | None:
    frames = [f for f in frames if not f.empty]
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    combined["source_file"] = source
    combined["run_id"] = run_id
    out_dir = settings.quarantine_dir / dataset
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{run_id}__{checksum[:12]}.csv"
    combined.to_csv(path, index=False)
    return path.as_posix()


def process_entry(settings: Settings, repo: MetadataRepository, run_id: str, entry: dict, rules: dict,
                  engine: str = "pandas") -> DatasetResult:
    """Process one validated manifest entry end-to-end."""
    dataset = entry["dataset"]
    spec = DATASETS[dataset]
    path = Path(entry["landed_path"])
    if engine == "spark":
        from src.processing.spark_processor import read_and_clean
        typed = read_and_clean(path, spec)
        malformed = scan_malformed(path, len(spec.columns))
    else:
        raw, malformed = read_raw_csv(path)
        typed = coerce_types(raw, spec)
    read_count = len(typed) + len(malformed)

    malformed_df = pd.DataFrame({"raw_line": malformed, "reject_reason": "malformed_row"})
    unique, dups = split_duplicates(typed, spec.key)
    context = {ref: read_table(settings, ref) for ref in REFERENCES.get(dataset, [])}
    valid, rejected, results = run_rules(dataset, unique, rules.get(dataset, []), context, stage="ingest")

    merged = upsert_table(settings, dataset, valid, spec.key) if not valid.empty or read_table(
        settings, dataset) is None else read_table(settings, dataset)
    quarantine = _write_quarantine(settings, dataset, run_id, entry["checksum"], entry["source_file"],
                                   [rejected, dups.drop(columns=["_parse_errors"], errors="ignore"), malformed_df])
    n_rejected = len(rejected) + len(dups) + len(malformed)
    repo.record_quality_results(run_id, results)
    repo.add_run_counts(run_id, len(valid), n_rejected)
    repo.upsert_dataset_metadata(dataset, len(merged), spec.column_names, entry["source_file"])
    repo.set_manifest_status(entry["source_file"], MANIFEST_PROCESSED)
    log.info("dataset processed", extra={"dataset": dataset, "read": read_count, "valid": len(valid),
                                         "rejected": n_rejected, "engine": engine})
    return DatasetResult(dataset, read_count, len(valid), n_rejected, len(dups), len(malformed), quarantine)


def process_pending(settings: Settings, repo: MetadataRepository, run_id: str,
                    engine: str = "pandas") -> list[DatasetResult]:
    """Process every manifest entry in 'validated' state, parents before children."""
    rules = load_rules(settings.quality_rules_path or None)
    order = {name: i for i, name in enumerate(DATASET_ORDER)}
    entries = sorted(repo.list_manifests(MANIFEST_VALIDATED), key=lambda e: order[e["dataset"]])
    results = []
    for entry in entries:
        try:
            results.append(process_entry(settings, repo, run_id, entry, rules, engine))
        except Exception as exc:
            repo.set_manifest_status(entry["source_file"], MANIFEST_FAILED, f"{type(exc).__name__}: {exc}")
            repo.record_error(run_id, "process", entry["dataset"], type(exc).__name__, str(exc))
            raise
    return results
