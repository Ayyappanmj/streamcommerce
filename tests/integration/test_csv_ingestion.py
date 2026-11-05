import pandas as pd

from src.metadata.models import MANIFEST_PROCESSED
from tests.helpers import make_settings, processed_snapshot, repo_for, run_batch, small_dataset


def test_clean_batch_end_to_end_without_rejections(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    summary = run_batch(s)
    assert summary["records_rejected"] == 0 and summary["rules_failed"] == 0
    snap = processed_snapshot(s)
    assert len(snap["orders"]) == 200 and len(snap["customers"]) == 40
    assert str(snap["orders"]["order_ts"].dtype).startswith("datetime64")
    repo = repo_for(s)
    assert {m["status"] for m in repo.list_manifests()} == {MANIFEST_PROCESSED}
    assert repo.get_run("test-run")["status"] == "success"
    assert (s.summaries_dir / "test-run.json").exists()
    assert repo.list_dataset_metadata()


def test_processed_layer_is_referentially_consistent(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s, faults=True)
    run_batch(s)
    snap = processed_snapshot(s)
    assert set(snap["orders"]["customer_id"]) <= set(snap["customers"]["customer_id"])
    assert set(snap["order_items"]["order_id"]) <= set(snap["orders"]["order_id"])
    assert set(snap["payments"]["order_id"]) <= set(snap["orders"]["order_id"])
    assert (snap["orders"]["order_total"] >= 0).all() and snap["orders"]["order_id"].is_unique


def test_failed_run_is_recorded(tmp_path):
    import pytest

    from src.common.exceptions import IngestionError
    s = make_settings(tmp_path)  # no input files at all
    with pytest.raises(IngestionError):
        run_batch(s, "bad-run")
    run = repo_for(s).get_run("bad-run")
    assert run["status"] == "failed" and "No CSV files" in run["details"]
    assert isinstance(pd.DataFrame(repo_for(s).list_errors("bad-run")), pd.DataFrame)
