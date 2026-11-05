import pandas as pd

from tests.helpers import make_settings, repo_for, run_batch, small_dataset


def test_deliberately_invalid_records_are_detected_quarantined_and_explained(tmp_path):
    s = make_settings(tmp_path)
    info = small_dataset(s, faults=True)
    faults = info["fault_report"]["tables"]
    summary = run_batch(s)
    assert summary["records_rejected"] > 0
    q = pd.concat([pd.read_csv(p, dtype=str) for p in (s.quarantine_dir / "orders").glob("*.csv")])
    reasons = " ".join(q["reject_reason"].dropna())
    for expected in ("orders_total_non_negative", "orders_required_fields", "orders_parseable_types",
                     "duplicate_key", "malformed_row"):
        assert expected in reasons, expected
    # every injected fault is accounted for in quarantine
    assert (q["reject_reason"] == "duplicate_key").sum() == faults["duplicate_orders"]
    assert (q["reject_reason"] == "malformed_row").sum() == faults["malformed_lines"]
    failed = {r["rule_name"] for r in repo_for(s).list_quality_results("test-run") if r["status"] == "failed"}
    assert {"orders_total_non_negative", "orders_required_fields", "orders_parseable_types"} <= failed


def test_every_quality_result_has_required_fields(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s, faults=True)
    run_batch(s)
    rows = repo_for(s).list_quality_results("test-run")
    assert rows
    for r in rows:
        assert r["dataset_name"] and r["rule_name"] and r["checked_at"] and r["status"] in ("passed", "failed", "skipped")
        assert r["failed_records"] <= max(r["total_records"], 0) or r["status"] == "skipped"
