"""Streaming event validation and idempotent persistence.

Delivery from Kafka is *at-least-once*: after a crash or rebalance the same message can be delivered again.
We therefore make persistence idempotent: ``event_id`` is the primary key of ``processed_events`` and rows are
written with INSERT OR IGNORE inside a transaction. Re-delivered events are counted as duplicates and do not
change analytical results ("effectively once" results; we do NOT claim end-to-end exactly-once delivery).

Business time is ``event_ts`` (set by the producer); pipeline time is ``ingested_at`` (set here).
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.common.schemas import CURRENCIES, EVENT_TYPES

log = logging.getLogger(__name__)
ORDER_ID_RE = re.compile(r"^ORD-\d{7,}$")
CUSTOMER_ID_RE = re.compile(r"^C\d{6,}$")
PROCESSED, DUPLICATE, QUARANTINED = "processed", "duplicate", "quarantined"

DDL = [
    """CREATE TABLE IF NOT EXISTS processed_events (
        event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, event_ts TEXT NOT NULL, ingested_at TEXT NOT NULL,
        order_id TEXT NOT NULL, customer_id TEXT, order_total REAL, refund_amount REAL, items_json TEXT,
        is_late INTEGER NOT NULL DEFAULT 0, source TEXT)""",
    """CREATE TABLE IF NOT EXISTS quarantined_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, raw_payload TEXT NOT NULL, reasons TEXT NOT NULL,
        received_at TEXT NOT NULL, source TEXT)""",
    """CREATE TABLE IF NOT EXISTS duplicate_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, seen_at TEXT NOT NULL, source TEXT)""",
]


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def parse_ts(value: object) -> datetime | None:
    """Parse an ISO-8601 timestamp (Z or offset) to an aware UTC datetime; None when invalid."""
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def validate_event(event: object) -> list[str]:
    """Return a list of validation errors (empty list = valid). See docs in DATA_DICTIONARY.md."""
    if not isinstance(event, dict):
        return ["event is not a JSON object"]
    errors: list[str] = []
    eid = event.get("event_id")
    if not isinstance(eid, str) or not eid.strip() or len(eid) > 128:
        errors.append("event_id missing or invalid")
    if event.get("event_type") not in EVENT_TYPES:
        errors.append(f"event_type must be one of {list(EVENT_TYPES)}")
    if parse_ts(event.get("event_ts")) is None:
        errors.append("event_ts missing or not ISO-8601")
    if event.get("schema_version") != 1:
        errors.append("unsupported schema_version")
    if not isinstance(event.get("order_id"), str) or not ORDER_ID_RE.match(event["order_id"]):
        errors.append("order_id missing or malformed")
    total = event.get("order_total")
    if not _is_number(total) or total < 0:
        errors.append("order_total must be a non-negative number")
    if event.get("currency") not in CURRENCIES:
        errors.append(f"currency must be one of {list(CURRENCIES)}")
    cust = event.get("customer_id")
    if event.get("event_type") == "order_created":
        if not isinstance(cust, str) or not CUSTOMER_ID_RE.match(cust):
            errors.append("customer_id missing or malformed")
        items = event.get("items")
        if not isinstance(items, list) or not items:
            errors.append("order_created requires at least one item")
        else:
            for i, item in enumerate(items):
                if not isinstance(item, dict) or not isinstance(item.get("product_id"), str):
                    errors.append(f"items[{i}].product_id invalid")
                elif not (isinstance(item.get("quantity"), int) and not isinstance(item.get("quantity"), bool)
                          and item["quantity"] > 0):
                    errors.append(f"items[{i}].quantity must be a positive integer")
                elif not _is_number(item.get("unit_price")) or item["unit_price"] < 0:
                    errors.append(f"items[{i}].unit_price must be non-negative")
    if event.get("event_type") == "order_refunded":
        amount = event.get("refund_amount")
        if not _is_number(amount) or amount < 0:
            errors.append("refund_amount must be a non-negative number")
    return errors


class EventStore:
    """SQLite-backed, transactional store for streaming results (standard library only)."""

    def __init__(self, path: Path, late_seconds: int = 3600) -> None:
        self.path = Path(path)
        self.late_seconds = late_seconds
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            for ddl in DDL:
                conn.execute(ddl)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def process(self, raw: str | bytes, source: str = "", ingested_at: datetime | None = None) -> str:
        """Validate and persist one message. Returns 'processed', 'duplicate' or 'quarantined'.

        Never raises for bad *data*; bad data is quarantined with reasons. Database errors do propagate
        so the caller can avoid committing the Kafka offset.
        """
        now = (ingested_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        try:
            event = json.loads(text)
        except json.JSONDecodeError as exc:
            return self._quarantine(text, [f"malformed JSON: {exc.msg}"], now, source)
        errors = validate_event(event)
        if errors:
            return self._quarantine(text, errors, now, source)
        event_ts = parse_ts(event["event_ts"])
        with self._connect() as conn:  # commits on success, rolls back on exception
            max_ts = conn.execute("SELECT MAX(event_ts) FROM processed_events").fetchone()[0]
            is_late = 0
            if max_ts:
                is_late = int(event_ts < parse_ts(max_ts) - timedelta(seconds=self.late_seconds))
            cur = conn.execute(
                "INSERT OR IGNORE INTO processed_events (event_id, event_type, event_ts, ingested_at, order_id, "
                "customer_id, order_total, refund_amount, items_json, is_late, source) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (event["event_id"], event["event_type"], event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"), now.isoformat(),
                 event["order_id"], event.get("customer_id"), float(event["order_total"]),
                 event.get("refund_amount"), json.dumps(event.get("items") or []), is_late, source))
            if cur.rowcount == 0:
                conn.execute("INSERT INTO duplicate_events (event_id, seen_at, source) VALUES (?,?,?)",
                             (event["event_id"], now.isoformat(), source))
                return DUPLICATE
        return PROCESSED

    def _quarantine(self, text: str, reasons: list[str], now: datetime, source: str) -> str:
        with self._connect() as conn:
            conn.execute("INSERT INTO quarantined_events (raw_payload, reasons, received_at, source) VALUES (?,?,?,?)",
                         (text[:10000], "; ".join(reasons), now.isoformat(), source))
        log.warning("event quarantined", extra={"reasons": reasons})
        return QUARANTINED

    def counts(self) -> dict[str, int]:
        with self._connect() as conn:
            q = lambda sql: conn.execute(sql).fetchone()[0]  # noqa: E731
            return {"processed": q("SELECT COUNT(*) FROM processed_events"),
                    "late": q("SELECT COUNT(*) FROM processed_events WHERE is_late = 1"),
                    "duplicates": q("SELECT COUNT(*) FROM duplicate_events"),
                    "quarantined": q("SELECT COUNT(*) FROM quarantined_events")}

    def stream_sales(self) -> float:
        """Gross order value from order_created events, counting each order once (earliest event wins)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(order_total), 0) FROM (SELECT order_id, order_total FROM processed_events "
                "WHERE event_type = 'order_created' GROUP BY order_id HAVING event_ts = MIN(event_ts))").fetchone()
        return round(float(row[0]), 2)

    def fetch_events(self) -> list[dict]:
        with self._connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM processed_events ORDER BY event_ts, event_id")]


def replay_lines(store: EventStore, lines: Iterable[str], source: str = "file") -> dict[str, int]:
    """Feed JSON lines through the same code path as the Kafka consumer (no broker required)."""
    tally = {PROCESSED: 0, DUPLICATE: 0, QUARANTINED: 0}
    for line in lines:
        if line.strip():
            tally[store.process(line, source=source)] += 1
    return tally
