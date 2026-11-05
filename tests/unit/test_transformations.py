import pandas as pd
import pytest

from src.common.schemas import DATASETS
from src.processing.transformations import (
    build_dim_date, build_fact_orders, build_warehouse_tables, coerce_types, stable_key)
from tests.helpers import make_settings, processed_snapshot, run_batch, small_dataset


def test_coerce_types_flags_unparseable_values_per_column():
    raw = pd.DataFrame({"order_id": ["ORD-0000001", "ORD-0000002"], "customer_id": ["C000001", "C000002"],
                        "order_ts": ["2025-01-01T10:00:00Z", "yesterday"], "status": ["paid", "paid"],
                        "currency": ["USD", "USD"], "order_total": ["10.5", "abc"]})
    out = coerce_types(raw, DATASETS["orders"])
    assert out.loc[0, "_parse_errors"] == "" and out.loc[0, "order_total"] == 10.5
    assert out.loc[1, "_parse_errors"] == "order_ts,order_total"
    assert str(out["order_ts"].dtype).startswith("datetime64")


def test_coerce_types_blank_is_null_not_parse_error_and_fractional_int_is_error():
    raw = pd.DataFrame({"order_item_id": ["a", "b"], "order_id": ["o", "o"], "product_id": ["", "p"],
                        "quantity": ["2", "2.5"], "unit_price": ["1", "1"], "line_total": [None, None]})
    out = coerce_types(raw, DATASETS["order_items"])
    assert pd.isna(out.loc[0, "product_id"]) and out.loc[0, "_parse_errors"] == ""
    assert out.loc[1, "_parse_errors"] == "quantity"


def test_stable_key_deterministic_and_distinct():
    assert stable_key("C000001") == stable_key("C000001") != stable_key("C000002")


def test_warehouse_tables_have_referential_integrity_and_correct_grain(tmp_path):
    s = make_settings(tmp_path)
    small_dataset(s)
    run_batch(s)
    t = build_warehouse_tables(processed_snapshot(s))
    fo, fi = t["fact_orders"], t["fact_order_items"]
    assert fo["order_key"].is_unique and fi["order_item_key"].is_unique  # grain
    assert set(fo["customer_key"]) <= set(t["dim_customers"]["customer_key"])
    assert set(fi["product_key"]) <= set(t["dim_products"]["product_key"])
    assert set(fi["order_key"]) <= set(fo["order_key"])
    keys = set(t["dim_date"]["date_key"])
    assert set(fo["order_date_key"]) <= keys and set(t["fact_payments"]["payment_date_key"]) <= keys
    assert fo["item_count"].sum() == len(fi)


def test_dim_date_is_contiguous():
    ts = pd.Series(pd.to_datetime(["2025-01-01T10:00:00Z", "2025-01-05T23:00:00Z"], utc=True))
    d = build_dim_date(ts)
    assert list(d["date_key"]) == [20250101, 20250102, 20250103, 20250104, 20250105]
    assert d.loc[0, "day_of_week"] == 3  # Wednesday


def test_fact_orders_without_items_gets_zero_counts():
    orders = pd.DataFrame({"order_id": ["ORD-0000001"], "customer_id": ["C000001"],
                           "order_ts": pd.to_datetime(["2025-01-01T00:00:00Z"], utc=True), "status": ["paid"],
                           "currency": ["USD"], "order_total": [5.0]})
    items = pd.DataFrame({"order_item_id": [], "order_id": [], "quantity": []})
    fact = build_fact_orders(orders, items)
    assert fact.loc[0, "item_count"] == 0 and fact.loc[0, "units"] == 0
    with pytest.raises(KeyError):
        build_fact_orders(orders, pd.DataFrame())
