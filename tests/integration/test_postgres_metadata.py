"""Needs PostgreSQL: set POSTGRES_TEST_PASSWORD (and optionally POSTGRES_TEST_HOST/PORT/USER/DB)."""
import os

import pytest

from src.common.config import Settings
from src.metadata.repository import MetadataRepository
from src.quality.quality_runner import QualityResult

pytestmark = pytest.mark.external
pytest.importorskip("psycopg")


@pytest.mark.skipif(not os.environ.get("POSTGRES_TEST_PASSWORD"), reason="POSTGRES_TEST_PASSWORD not set")
def test_postgres_metadata_roundtrip(tmp_path):
    s = Settings.for_data_dir(
        tmp_path, metadata_backend="postgres", postgres_password=os.environ["POSTGRES_TEST_PASSWORD"],
        postgres_host=os.environ.get("POSTGRES_TEST_HOST", "localhost"),
        postgres_port=int(os.environ.get("POSTGRES_TEST_PORT", "5432")),
        postgres_user=os.environ.get("POSTGRES_TEST_USER", "streamcommerce"),
        postgres_db=os.environ.get("POSTGRES_TEST_DB", "streamcommerce"))
    repo = MetadataRepository(s)
    repo.init_schema()
    run_id = f"pg-test-{os.urandom(3).hex()}"
    repo.start_run(run_id, "batch", "pandas")
    repo.record_quality_results(run_id, [QualityResult("orders", "r", "error", 10, 1, "failed", "x", "2025-01-01T00:00:00Z")])
    repo.upsert_manifest("f.csv", "orders", "abc", 3, "ingested", None, run_id)
    repo.finish_run(run_id, "success")
    assert repo.get_run(run_id)["status"] == "success"
    assert repo.list_quality_results(run_id)[0]["failed_records"] == 1
    assert repo.get_manifest("f.csv")["checksum"] == "abc"
