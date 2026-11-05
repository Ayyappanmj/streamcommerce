import json
from datetime import datetime, timedelta, timezone

from src.processing.event_processor import DUPLICATE, PROCESSED, QUARANTINED, EventStore, validate_event


def _event(i=1, **over):
    base = {"event_id": f"e-{i}", "event_type": "order_created", "event_ts": "2025-01-01T10:00:00Z",
            "schema_version": 1, "order_id": f"ORD-{i:07d}", "customer_id": "C000001", "order_total": 20.0,
            "currency": "USD", "items": [{"product_id": "P00001", "quantity": 2, "unit_price": 10.0}],
            "refund_amount": None}
    base.update(over)
    return base


def _store(tmp_path, late=3600):
    return EventStore(tmp_path / "events.sqlite", late)


def test_valid_event_is_processed_then_duplicate_ignored(tmp_path):
    s = _store(tmp_path)
    line = json.dumps(_event())
    assert s.process(line) == PROCESSED
    assert s.process(line) == DUPLICATE
    assert s.counts()["processed"] == 1 and s.counts()["duplicates"] == 1
    assert s.stream_sales() == 20.0


def test_invalid_events_are_quarantined_with_reasons(tmp_path):
    s = _store(tmp_path)
    bad = [_event(2, order_total=-1), _event(3, customer_id=None), _event(4, event_ts="nope"),
           _event(5, event_type="order_teleported"), _event(6, items=[]),
           _event(7, items=[{"product_id": "P1", "quantity": 0, "unit_price": 1}]),
           _event(8, event_type="order_refunded", refund_amount=-2.0)]
    for e in bad:
        assert s.process(json.dumps(e)) == QUARANTINED
    assert s.process('{"event_id": "x", "trunc') == QUARANTINED
    assert s.process(b"\xff\xfe") == QUARANTINED
    assert s.process("[1, 2]") == QUARANTINED
    c = s.counts()
    assert c["processed"] == 0 and c["quarantined"] == 10
    with s._connect() as conn:
        reasons = " | ".join(r[0] for r in conn.execute("SELECT reasons FROM quarantined_events"))
    assert "order_total" in reasons and "customer_id" in reasons and "event_ts" in reasons and "malformed JSON" in reasons


def test_validate_event_accepts_generated_shape_and_reports_multiple_errors():
    assert validate_event(_event()) == []
    errors = validate_event({"event_type": "order_created"})
    assert len(errors) >= 4


def test_duplicates_do_not_inflate_totals_even_with_new_event_id_for_same_order(tmp_path):
    s = _store(tmp_path)
    s.process(json.dumps(_event(1)))
    s.process(json.dumps(_event(1)))  # redelivery
    s.process(json.dumps(_event(1, event_id="e-1-retry", event_ts="2025-01-01T10:05:00Z")))  # producer retry, new id
    assert s.stream_sales() == 20.0  # order counted once


def test_late_events_are_flagged_but_still_processed_and_ingestion_time_is_separate(tmp_path):
    s = _store(tmp_path, late=3600)
    now = datetime(2025, 6, 1, tzinfo=timezone.utc)
    s.process(json.dumps(_event(1, event_ts="2025-01-02T12:00:00Z")), ingested_at=now)
    s.process(json.dumps(_event(2, event_ts="2025-01-02T11:30:00Z")), ingested_at=now)  # 30 min behind: not late
    s.process(json.dumps(_event(3, event_ts="2025-01-01T12:00:00Z")), ingested_at=now)  # a day behind: late
    by_id = {e["event_id"]: e for e in s.fetch_events()}
    assert [by_id[f"e-{i}"]["is_late"] for i in (1, 2, 3)] == [0, 0, 1]
    assert by_id["e-3"]["event_ts"] == "2025-01-01T12:00:00Z"
    assert by_id["e-3"]["ingested_at"].startswith("2025-06-01")


def test_out_of_order_arrival_gives_same_totals(tmp_path):
    events = [_event(i, event_ts=(datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i)).strftime(
        "%Y-%m-%dT%H:%M:%SZ")) for i in range(1, 6)]
    a, b = EventStore(tmp_path / "a.sqlite"), EventStore(tmp_path / "b.sqlite")
    for e in events:
        a.process(json.dumps(e))
    for e in reversed(events):
        b.process(json.dumps(e))
    assert a.stream_sales() == b.stream_sales() == 100.0 and a.counts()["processed"] == b.counts()["processed"] == 5


def test_restart_with_redelivery_does_not_change_results(tmp_path):
    lines = [json.dumps(_event(i)) for i in range(1, 6)]
    first = EventStore(tmp_path / "e.sqlite")
    for line in lines[:3]:
        first.process(line)
    restarted = EventStore(tmp_path / "e.sqlite")  # new process, same file; broker redelivers from last commit
    for line in lines[1:]:
        restarted.process(line)
    assert restarted.counts()["processed"] == 5 and restarted.stream_sales() == 100.0
    assert restarted.counts()["duplicates"] == 2
