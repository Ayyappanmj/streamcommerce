import os
from pathlib import Path
import shutil
import sys

import pytest

from tests.helpers import make_settings, small_dataset

pytestmark = pytest.mark.external
pytest.importorskip("duckdb")


def test_dbt_build_runs_models_and_tests_on_local_warehouse(tmp_path):
    dbt = os.environ.get("DBT_EXECUTABLE") or shutil.which("dbt")
    if not dbt:
        candidate = Path(sys.prefix) / ("Scripts" if sys.platform == "win32" else "bin") / ("dbt.exe" if sys.platform == "win32" else "dbt")
        if candidate.exists():
            dbt = str(candidate)
    if not dbt:
        pytest.skip("dbt not installed")
    import duckdb

    from src.batch_pipeline import run_pipeline
    s = make_settings(tmp_path)
    small_dataset(s)
    run_pipeline(s, ["prepare", "ingest", "validate", "process", "load_warehouse", "dbt"], run_id="dbt-run")
    con = duckdb.connect(str(s.warehouse_path), read_only=True)
    assert con.execute("SELECT count(*) FROM analytics.daily_sales").fetchone()[0] > 0
    gross = con.execute("SELECT sum(gross_sales) FROM analytics.daily_sales").fetchone()[0]
    orders = con.execute("SELECT sum(order_total) FROM dw.fact_orders WHERE status <> 'cancelled'").fetchone()[0]
    assert gross == pytest.approx(orders, abs=0.01)
    con.close()
