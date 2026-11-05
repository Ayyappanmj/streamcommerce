import pytest

from src.common.exceptions import SchemaValidationError
from src.common.schemas import DATASETS
from src.ingestion.csv_ingestion import read_raw_csv, validate_header
from src.quality.validators import detect_schema_drift


def test_missing_required_column_raises(tmp_path):
    p = tmp_path / "orders.csv"
    p.write_text("order_id,customer_id,order_ts,status,currency\nORD-0000001,C000001,2025-01-01T00:00:00Z,paid,USD\n")
    with pytest.raises(SchemaValidationError, match="order_total"):
        validate_header(p, DATASETS["orders"])


def test_unexpected_column_reported_as_drift_not_failure(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_text("customer_id,email,country,segment,signup_ts,loyalty_tier\n")
    drift = validate_header(p, DATASETS["customers"])
    assert drift["unexpected"] == ["loyalty_tier"] and drift["missing"] == []


def test_empty_file_raises(tmp_path):
    p = tmp_path / "products.csv"
    p.write_text("")
    with pytest.raises(SchemaValidationError, match="empty"):
        validate_header(p, DATASETS["products"])


def test_malformed_lines_are_returned_not_dropped_silently(tmp_path):
    p = tmp_path / "refunds.csv"
    p.write_text("refund_id,order_id,amount,refund_ts,reason\nREF-1,ORD-0000001,1.0,2025-01-01T00:00:00Z,x\n"
                 "BAD,1,2,3,4,5,6,7\n")
    df, bad = read_raw_csv(p)
    assert len(df) == 1 and len(bad) == 1 and bad[0].startswith("BAD")


def test_detect_schema_drift():
    assert detect_schema_drift(["a", "x"], ["a", "b"]) == {"missing": ["b"], "unexpected": ["x"]}
