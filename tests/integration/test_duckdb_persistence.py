import pytest

from tests.helpers import make_settings, repo_for, run_batch, small_dataset

duckdb = pytest.importorskip("duckdb")


def _counts(path):
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = [r[0] for r in con.execute(
            "SELECT schema_name || '.' || table_name FROM duckdb_tables() ORDER BY 1").fetchall()]
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}
    finally:
        con.close()


def test_warehouse_contains_expected_tables_and_enforces_keys(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    run_batch(s, steps=["prepare", "ingest", "validate", "process", "load_warehouse", "final_quality", "summary"])
    counts = _counts(s.warehouse_path)
    for t in ("dw.dim_customers", "dw.dim_products", "dw.dim_date", "dw.fact_orders", "dw.fact_order_items",
              "dw.fact_payments", "dw.fact_refunds", "raw.orders", "raw.quality_results"):
        assert t in counts, t
    assert counts["dw.fact_orders"] == 200
    con = duckdb.connect(str(s.warehouse_path))
    with pytest.raises(duckdb.Error):
        con.execute("INSERT INTO dw.fact_payments VALUES (1, 'P', -1, 20250101, 5, 'card', 'captured', NULL)")
    total = con.execute("SELECT sum(order_total) FROM dw.fact_orders WHERE status <> 'cancelled'").fetchone()[0]
    items = con.execute("SELECT sum(i.line_total) FROM dw.fact_order_items i JOIN dw.fact_orders o USING (order_key) "
                        "WHERE o.status <> 'cancelled'").fetchone()[0]
    assert total == pytest.approx(items, abs=0.05)
    con.close()


def test_reloading_warehouse_is_idempotent(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    run_batch(s, steps=["prepare", "ingest", "validate", "process", "load_warehouse"])
    first = _counts(s.warehouse_path)
    from src.warehouse.load_warehouse import load_warehouse
    load_warehouse(s, repo_for(s))
    load_warehouse(s, repo_for(s))
    assert _counts(s.warehouse_path) == first
