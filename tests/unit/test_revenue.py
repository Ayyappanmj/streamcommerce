import pandas as pd
import pytest

from src.processing.transformations import build_fact_orders, total_sales
from tests.helpers import make_settings, processed_snapshot, run_batch, small_dataset


def test_naive_join_multiplies_revenue_but_order_grain_does_not(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    run_batch(s)
    snap = processed_snapshot(s)
    orders, items = snap["orders"], snap["order_items"]
    live = orders[orders["status"] != "cancelled"]
    truth = round(float(live["order_total"].sum()), 2)

    naive = orders.merge(items, on="order_id")  # one row per ITEM carries the whole order_total
    naive_total = float(naive.loc[naive["status"] != "cancelled", "order_total"].sum())
    assert naive_total > truth * 1.2  # demonstrates the double counting hazard

    fact = build_fact_orders(orders, items)
    assert total_sales(fact) == pytest.approx(truth, abs=0.01)
    # item-grain revenue reconciles with order-grain revenue
    item_total = items.merge(live[["order_id"]], on="order_id")["line_total"].sum()
    assert item_total == pytest.approx(truth, abs=0.05)


def test_total_sales_excludes_cancelled_orders():
    f = pd.DataFrame({"status": ["paid", "cancelled", "delivered"], "order_total": [10.0, 99.0, 5.5]})
    assert total_sales(f) == 15.5
