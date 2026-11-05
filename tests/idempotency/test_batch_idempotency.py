import pandas as pd
import pytest

from src.generators.cli import generate_dataset
from src.processing.event_processor import EventStore, replay_lines
from tests.helpers import make_settings, processed_snapshot, repo_for, run_batch, small_dataset


def test_same_input_twice_leaves_processed_layer_unchanged(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s, faults=True)
    run_batch(s, "run-1")
    first = processed_snapshot(s)
    run_batch(s, "run-2")  # unchanged files skipped by manifest
    run_batch(s, "run-3", force=True)  # forced reprocessing must still not duplicate
    second = processed_snapshot(s)
    for name in first:
        pd.testing.assert_frame_equal(first[name], second[name], check_like=False)


def test_unchanged_files_are_skipped_on_rerun(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    run_batch(s, "run-1")
    summary = run_batch(s, "run-2")
    assert all(i["skipped"] for i in summary["step_details"]["ingest"])
    assert summary["records_processed"] == 0


def test_new_batch_with_overlapping_rows_upserts_not_duplicates(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s, orders=200, seed=7)
    run_batch(s, "run-1")
    n_orders = len(processed_snapshot(s)["orders"])
    # regenerate a superset with the same seed: first 200 orders are identical, 50 are new
    generate_dataset(s.incoming_dir, customers=40, products=15, orders=250, seed=7)
    run_batch(s, "run-2")
    snap = processed_snapshot(s)
    assert snap["orders"]["order_id"].is_unique and snap["order_items"]["order_item_id"].is_unique
    assert len(snap["orders"]) >= n_orders


def test_event_replay_twice_leaves_totals_unchanged(tmp_path):
    s = make_settings(tmp_path)
    generate_dataset(tmp_path / "c", 30, 10, 80, 3, faults=True, events_file=tmp_path / "e.jsonl")
    lines = (tmp_path / "e.jsonl").read_text().splitlines()
    store = EventStore(s.events_db_path)
    replay_lines(store, lines)
    before = (store.counts()["processed"], store.stream_sales(), len(store.fetch_events()))
    replay_lines(store, lines)
    after = (store.counts()["processed"], store.stream_sales(), len(store.fetch_events()))
    assert before == after


def test_warehouse_tables_identical_after_second_run(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    s = make_settings(tmp_path)
    small_dataset(s)
    steps = ["prepare", "ingest", "validate", "process", "load_warehouse"]
    run_batch(s, "r1", steps=steps)

    def dump():
        con = duckdb.connect(str(s.warehouse_path), read_only=True)
        try:
            return {t: con.execute(f"SELECT * FROM dw.{t} ORDER BY 1").df()
                    for t in ("fact_orders", "fact_order_items", "fact_payments", "dim_customers")}
        finally:
            con.close()
    a = dump()
    run_batch(s, "r2", force=True, steps=steps)
    b = dump()
    for t in a:
        pd.testing.assert_frame_equal(a[t], b[t])
    assert repo_for(s).get_run("r2")["status"] == "success"
