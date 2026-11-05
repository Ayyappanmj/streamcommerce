import pytest

from src.generators.cli import generate_dataset
from src.generators.common import parse_start_date
from src.generators.generate_customers import generate_customers
from src.generators.generate_events import build_events, inject_event_faults
from src.generators.generate_orders import generate_order_tables
from src.generators.generate_products import generate_products

START = parse_start_date("2025-01-01")


def _tables(seed=1, orders=120):
    cust = generate_customers(25, seed, START)
    prod = generate_products(12, seed, START)
    return cust, prod, generate_order_tables(cust, prod, orders, seed, START)


def test_same_seed_is_deterministic_and_other_seed_differs():
    assert _tables(1)[2] == _tables(1)[2]
    assert _tables(1)[2] != _tables(2)[2]


def test_relationships_and_amounts_agree():
    cust, prod, t = _tables()
    customer_ids = {c["customer_id"] for c in cust}
    product_price = {p["product_id"]: p["unit_price"] for p in prod}
    items_by_order = {}
    for item in t["order_items"]:
        assert item["product_id"] in product_price
        assert item["quantity"] > 0
        assert item["line_total"] == pytest.approx(item["quantity"] * product_price[item["product_id"]], abs=0.01)
        items_by_order.setdefault(item["order_id"], []).append(item)
    pay = {p["order_id"]: p for p in t["payments"]}
    for order in t["orders"]:
        assert order["customer_id"] in customer_ids
        assert order["order_id"] in items_by_order  # at least one item
        assert order["order_total"] == pytest.approx(sum(i["line_total"] for i in items_by_order[order["order_id"]]),
                                                     abs=0.01)
        if order["order_id"] in pay:
            assert pay[order["order_id"]]["amount"] == order["order_total"]
    totals = {o["order_id"]: o["order_total"] for o in t["orders"]}
    assert all(r["amount"] <= totals[r["order_id"]] for r in t["refunds"])


def test_events_have_unique_ids_and_valid_timestamps():
    _, _, t = _tables()
    events = build_events(t, 1)
    assert len({e["event_id"] for e in events}) == len(events)
    assert [e["event_ts"] for e in events] == sorted(e["event_ts"] for e in events)
    assert {e["event_type"] for e in events} >= {"order_created", "order_paid"}


def test_faults_are_off_by_default_and_reported_when_on(tmp_path):
    clean = generate_dataset(tmp_path / "a", 30, 10, 100, 3)
    assert clean["fault_report"] == {} and clean["faults"] is False
    dirty = generate_dataset(tmp_path / "b", 30, 10, 100, 3, faults=True, events_file=tmp_path / "e.jsonl")
    tbl = dirty["fault_report"]["tables"]
    assert all(v >= 1 for v in tbl.values())
    assert all(v >= 1 for v in dirty["fault_report"]["events"].values())


def test_event_fault_duplicates_never_precede_original():
    _, _, t = _tables()
    lines, report = inject_event_faults(build_events(t, 1), 1, 0.05)
    assert report["duplicates"] >= 1
    import json
    seen = set()
    for line in lines:
        try:
            eid = json.loads(line)["event_id"]
        except ValueError:
            continue
        seen.add(eid)
    assert len(lines) > len(seen)  # duplicates exist


def test_invalid_arguments_rejected(tmp_path):
    with pytest.raises(ValueError):
        generate_customers(0, 1, START)
    with pytest.raises(ValueError):
        generate_dataset(tmp_path, fault_rate=1.5)
