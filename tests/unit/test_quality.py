import pandas as pd
import pytest

from src.common.exceptions import DataQualityError
from src.common.schemas import DATASETS
from src.processing.transformations import coerce_types
from src.quality.quality_rules import DEFAULT_RULES
from src.quality.quality_runner import run_rules
from src.quality.validators import get_validator


def _orders(rows):
    raw = pd.DataFrame(rows, columns=["order_id", "customer_id", "order_ts", "status", "currency", "order_total"])
    return coerce_types(raw, DATASETS["orders"])


def test_invalid_rows_are_rejected_with_reasons_and_valid_rows_kept():
    df = _orders([
        ["ORD-0000001", "C000001", "2025-01-01T00:00:00Z", "paid", "USD", "10"],
        ["ORD-0000002", "C000001", "2025-01-01T00:00:00Z", "paid", "USD", "-5"],
        ["ORD-0000003", None, "2025-01-01T00:00:00Z", "paid", "USD", "5"],
        ["ORD-0000004", "C000001", "garbage", "paid", "USD", "5"],
        ["BAD", "C000001", "2025-01-01T00:00:00Z", "paid", "USD", "5"],
        ["ORD-0000006", "C000001", "2025-01-01T00:00:00Z", "weird", "USD", "5"],
    ])
    ctx = {"customers": pd.DataFrame({"customer_id": ["C000001"]})}
    valid, rejected, results = run_rules("orders", df, DEFAULT_RULES["orders"], ctx)
    assert list(valid["order_id"]) == ["ORD-0000001"]
    reasons = dict(zip(rejected["order_id"], rejected["reject_reason"], strict=True))
    assert "orders_total_non_negative" in reasons["ORD-0000002"]
    assert "orders_required_fields" in reasons["ORD-0000003"]
    assert "orders_parseable_types" in reasons["ORD-0000004"]
    assert "orders_id_format" in reasons["BAD"]
    assert "orders_status_allowed" in reasons["ORD-0000006"]
    assert all(r.total_records in (0, 6) for r in results)
    failed = {r.rule_name for r in results if r.status == "failed"}
    assert {"orders_total_non_negative", "orders_required_fields"} <= failed


def test_unknown_customer_reference_rejected_and_unavailable_reference_skipped():
    df = _orders([["ORD-0000001", "C999999", "2025-01-01T00:00:00Z", "paid", "USD", "10"]])
    valid, rejected, _ = run_rules("orders", df, DEFAULT_RULES["orders"],
                                   {"customers": pd.DataFrame({"customer_id": ["C000001"]})})
    assert valid.empty and "orders_customer_exists" in rejected.iloc[0]["reject_reason"]
    valid, rejected, results = run_rules("orders", df, DEFAULT_RULES["orders"], {"customers": None})
    assert len(valid) == 1
    assert [r.status for r in results if r.rule_name == "orders_customer_exists"] == ["skipped"]


def test_warn_rules_record_failures_without_rejecting():
    orders = _orders([["ORD-0000001", "C000001", "2025-01-01T00:00:00Z", "paid", "USD", "10"]])
    items = pd.DataFrame({"order_id": ["ORD-0000001"], "line_total": [7.0]})
    valid, rejected, results = run_rules("orders", orders, DEFAULT_RULES["orders"], {"order_items": items},
                                         stage="final")
    assert len(valid) == 1 and rejected.empty
    res = next(r for r in results if r.rule_name == "orders_total_matches_items")
    assert res.status == "failed" and res.failed_records == 1 and res.severity == "warn"


def test_positive_and_non_negative_validators():
    df = pd.DataFrame({"q": [1, 0, -1], "p": [0.0, -0.01, 5.0]})
    assert list(get_validator("positive")(df, column="q")) == [False, True, True]
    assert list(get_validator("non_negative")(df, column="p")) == [False, True, False]


def test_unknown_rule_type_raises():
    with pytest.raises(DataQualityError):
        get_validator("nope")
