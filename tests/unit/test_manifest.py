import pytest

from src.common.exceptions import SchemaValidationError
from src.ingestion.csv_ingestion import file_checksum, ingest_directory, validate_pending
from src.metadata.models import MANIFEST_FAILED, MANIFEST_INGESTED, MANIFEST_PROCESSED
from tests.helpers import make_settings, repo_for, run_batch, small_dataset


def test_repository_roundtrip_and_run_lifecycle(tmp_path):
    s = make_settings(tmp_path)
    repo = repo_for(s)
    repo.start_run("r1", "batch", "pandas")
    repo.start_run("r1", "batch", "pandas")  # idempotent
    repo.add_run_counts("r1", 10, 2)
    repo.finish_run("r1", "success")
    run = repo.get_run("r1")
    assert run["status"] == "success" and run["records_processed"] == 10 and run["finished_at"]
    assert len(repo.list_runs()) == 1


def test_manifest_records_checksum_and_unchanged_file_is_skipped_after_processing(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    repo = repo_for(s)
    first = ingest_directory(s, repo, "r1")
    assert not any(f.skipped for f in first)
    manifest = repo.get_manifest((s.incoming_dir / "orders.csv").resolve().as_posix())
    assert manifest["checksum"] == file_checksum(s.incoming_dir / "orders.csv")
    assert manifest["status"] == MANIFEST_INGESTED and manifest["record_count"] == 200
    # not yet processed -> still not skipped
    assert not any(f.skipped for f in ingest_directory(s, repo, "r2"))
    run_batch(s, "r3")
    again = ingest_directory(s, repo, "r4")
    assert all(f.skipped for f in again)
    forced = ingest_directory(s, repo, "r5", force=True)
    assert not any(f.skipped for f in forced)


def test_changed_file_is_reprocessed(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    run_batch(s, "r1")
    path = s.incoming_dir / "refunds.csv"
    path.write_text(path.read_text() + "REF-999999,ORD-0000001,1.00,2025-02-01T00:00:00Z,test\n")
    repo = repo_for(s)
    res = {f.dataset: f.skipped for f in ingest_directory(s, repo, "r2")}
    assert res["refunds"] is False and res["orders"] is True


def test_corrupted_file_is_recorded_as_failed_not_skipped(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    (s.incoming_dir / "orders.csv").write_text("order_id,customer_id\nORD-0000001,C000001\n")
    repo = repo_for(s)
    ingest_directory(s, repo, "r1")
    with pytest.raises(SchemaValidationError):
        validate_pending(repo, "r1")
    entry = repo.get_manifest((s.incoming_dir / "orders.csv").resolve().as_posix())
    assert entry["status"] == MANIFEST_FAILED and "missing required columns" in entry["error_details"]
    assert repo.list_errors("r1")[0]["error_type"] == "SchemaValidationError"
    assert entry["status"] != MANIFEST_PROCESSED
