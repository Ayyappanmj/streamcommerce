"""Build streaming order events (JSON wire format) from generated tables, with optional faults."""
from __future__ import annotations

import json
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

from src.generators.common import iso

SCHEMA_VERSION = 1


def _event_id(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def build_events(tables: dict[str, list[dict]], seed: int) -> list[dict]:
    """Create valid events (order_created, order_paid, order_shipped, order_cancelled, order_refunded)
    sorted by event timestamp. Event IDs are deterministic for a given seed."""
    rng = random.Random(f"events-{seed}")
    items_by_order: dict[str, list[dict]] = defaultdict(list)
    for item in tables["order_items"]:
        items_by_order[item["order_id"]].append(
            {"product_id": item["product_id"], "quantity": item["quantity"], "unit_price": item["unit_price"]})
    orders = {o["order_id"]: o for o in tables["orders"]}
    events: list[dict] = []

    def make(kind: str, ts: str, order: dict, **extra: object) -> dict:
        base = {"event_id": _event_id(rng), "event_type": kind, "event_ts": ts, "schema_version": SCHEMA_VERSION,
                "order_id": order["order_id"], "customer_id": order["customer_id"],
                "order_total": order["order_total"], "currency": order["currency"],
                "items": [], "refund_amount": None}
        base.update(extra)
        return base

    for order in tables["orders"]:
        order_ts = datetime.strptime(order["order_ts"], "%Y-%m-%dT%H:%M:%SZ")
        events.append(make("order_created", order["order_ts"], order, items=items_by_order[order["order_id"]]))
        if order["status"] == "cancelled":
            events.append(make("order_cancelled", iso(order_ts.replace(tzinfo=None) + timedelta(hours=1)), order))
        if order["status"] in ("shipped", "delivered"):
            events.append(make("order_shipped", iso(order_ts.replace(tzinfo=None) + timedelta(days=1)), order))
    for pay in tables["payments"]:
        events.append(make("order_paid", pay["payment_ts"], orders[pay["order_id"]]))
    for ref in tables["refunds"]:
        events.append(make("order_refunded", ref["refund_ts"], orders[ref["order_id"]], refund_amount=ref["amount"]))
    events.sort(key=lambda e: (e["event_ts"], e["event_id"]))
    return events


def to_wire(event: dict) -> str:
    """Serialise one event as compact JSON."""
    return json.dumps(event, sort_keys=True, separators=(",", ":"))


def inject_event_faults(events: list[dict], seed: int, rate: float = 0.02) -> tuple[list[str], dict[str, int]]:
    """Return wire-format lines (publish order) and a report of injected faults.

    Faults: duplicates (same event_id re-sent), negative amounts, missing customer IDs,
    malformed JSON, late events (event_ts moved 2 days back) and out-of-order neighbours.
    """
    rng = random.Random(f"event-faults-{seed}")
    evs = [dict(e) for e in events]
    n = len(evs)
    k = max(1, int(n * rate)) if n else 0
    report = dict.fromkeys(("duplicates", "invalid_amounts", "missing_customer_ids", "malformed",
                            "late_events", "out_of_order"), 0)
    created = [i for i, e in enumerate(evs) if e["event_type"] == "order_created"]
    for i in rng.sample(created, min(len(created), k)):
        evs[i]["order_total"] = -abs(evs[i]["order_total"]) - 1
        report["invalid_amounts"] += 1
    for i in rng.sample(created, min(len(created), k)):
        if evs[i]["order_total"] >= 0:
            evs[i]["customer_id"] = None
            report["missing_customer_ids"] += 1
    for i in rng.sample(range(n), min(n, k)):
        if evs[i]["order_total"] >= 0 and evs[i]["customer_id"]:
            ts = datetime.strptime(evs[i]["event_ts"], "%Y-%m-%dT%H:%M:%SZ")
            evs[i]["event_ts"] = iso(ts - timedelta(days=2))
            report["late_events"] += 1
    lines = [to_wire(e) for e in evs]
    for i in rng.sample(range(0, max(1, len(lines) - 1), 1), min(len(lines) // 2, k)):
        lines[i], lines[i + 1] = lines[i + 1], lines[i]
        report["out_of_order"] += 1
    # Re-deliveries arrive AFTER the original (like a producer retry or consumer replay).
    keyed = [(float(i), line) for i, line in enumerate(lines)]
    dup_idx = rng.sample(range(len(lines)), min(len(lines), k)) if lines else []
    for i in dup_idx:
        keyed.append((i + rng.uniform(0.1, 25.0), lines[i]))
    report["duplicates"] = len(dup_idx)
    for _ in range(max(1, k // 2) if lines else 0):
        victim = rng.choice(lines)
        keyed.append((rng.uniform(0, len(lines)), victim[: len(victim) // 2]))
        report["malformed"] += 1
    keyed.sort(key=lambda pair: pair[0])
    lines = [line for _, line in keyed]
    return lines, report
