"""CSV ingestion: checksum, manifest check, structure validation, landing."""
from __future__ import annotations

import csv
import hashlib
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.common.config import Settings
from src.common.exceptions import IngestionError, SchemaValidationError
from src.common.schemas import DATASET_ORDER, DATASETS, DatasetSpec
from src.metadata.models import MANIFEST_FAILED, MANIFEST_INGESTED, MANIFEST_PROCESSED, MANIFEST_VALIDATED
from src.metadata.repository import MetadataRepository
from src.quality.validators import detect_schema_drift

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestedFile:
    dataset: str
    source_file: str
    checksum: str
    landed_path: Path | None
    record_count: int
    skipped: bool
    reason: str = ""


def file_checksum(path: Path) -> str:
    """SHA-256 of the file contents."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_header(path: Path) -> list[str]:
    """Return the CSV header, raising :class:`SchemaValidationError` for empty/unreadable files."""
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            header = next(csv.reader(handle), None)
    except (UnicodeDecodeError, csv.Error) as exc:
        raise SchemaValidationError(f"{path.name}: file is not valid UTF-8 CSV ({exc})") from exc
    if not header:
        raise SchemaValidationError(f"{path.name}: file is empty (no header row)")
    return [h.strip() for h in header]


def validate_header(path: Path, spec: DatasetSpec) -> dict[str, list[str]]:
    """Ensure all required columns exist. Returns schema drift (extra columns are reported, not fatal)."""
    drift = detect_schema_drift(read_header(path), spec.column_names)
    missing_required = [c for c in drift["missing"] if c in spec.required]
    if missing_required:
        raise SchemaValidationError(f"{path.name}: missing required columns {missing_required}")
    return drift


def count_data_rows(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8", errors="replace") as handle:
        return max(sum(1 for _ in csv.reader(handle)) - 1, 0)


def read_raw_csv(path: Path) -> tuple[pd.DataFrame, list[str]]:
    """Read a CSV as strings. Lines with too many fields are returned (not dropped silently)."""
    malformed: list[str] = []

    def _collect(fields: list[str]) -> None:
        malformed.append(",".join(fields))

    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""], engine="python",
                     on_bad_lines=_collect)
    df.columns = [c.strip() for c in df.columns]
    return df, malformed


def ingest_directory(settings: Settings, repo: MetadataRepository, run_id: str, input_dir: Path | None = None,
                     force: bool = False) -> list[IngestedFile]:
    """Register and land every known dataset file found in ``input_dir``.

    A file is skipped only when the manifest shows the *same checksum* was already fully processed.
    Missing files are logged; unreadable files are recorded as failed and raise :class:`IngestionError`.
    """
    src_dir = input_dir or settings.incoming_dir
    settings.landed_dir.mkdir(parents=True, exist_ok=True)
    results: list[IngestedFile] = []
    for name in DATASET_ORDER:
        path = src_dir / f"{name}.csv"
        if not path.is_file():
            log.warning("expected file not found", extra={"dataset": name, "path": str(path)})
            continue
        source = path.resolve().as_posix()
        try:
            checksum = file_checksum(path)
            rows = count_data_rows(path)
        except OSError as exc:
            repo.upsert_manifest(source, name, "", 0, MANIFEST_FAILED, None, run_id, str(exc))
            raise IngestionError(f"Cannot read {path}: {exc}") from exc
        manifest = repo.get_manifest(source)
        if manifest and manifest["checksum"] == checksum and manifest["status"] == MANIFEST_PROCESSED and not force:
            log.info("unchanged file skipped", extra={"dataset": name, "checksum": checksum[:12]})
            results.append(IngestedFile(name, source, checksum, None, rows, True, "unchanged"))
            continue
        landed = settings.landed_dir / f"{name}__{checksum[:12]}.csv"
        if not landed.exists():
            shutil.copyfile(path, landed)
        repo.upsert_manifest(source, name, checksum, rows, MANIFEST_INGESTED, landed.as_posix(), run_id)
        results.append(IngestedFile(name, source, checksum, landed, rows, False))
    return results


def validate_pending(repo: MetadataRepository, run_id: str) -> list[str]:
    """Validate the structure of all manifests in 'ingested' state; mark them validated or failed.

    Returns warnings about schema drift. Raises :class:`SchemaValidationError` after recording failures.
    """
    warnings: list[str] = []
    failures: list[str] = []
    for entry in repo.list_manifests(MANIFEST_INGESTED):
        spec = DATASETS[entry["dataset"]]
        try:
            drift = validate_header(Path(entry["landed_path"]), spec)
        except SchemaValidationError as exc:
            repo.set_manifest_status(entry["source_file"], MANIFEST_FAILED, str(exc))
            repo.record_error(run_id, "validate", entry["dataset"], "SchemaValidationError", str(exc))
            failures.append(str(exc))
            continue
        if drift["unexpected"] or drift["missing"]:
            msg = f"{entry['dataset']}: schema drift {drift}"
            warnings.append(msg)
            log.warning("schema drift", extra={"dataset": entry["dataset"], "drift": drift})
        repo.set_manifest_status(entry["source_file"], MANIFEST_VALIDATED)
    if failures:
        raise SchemaValidationError("; ".join(failures))
    return warnings
