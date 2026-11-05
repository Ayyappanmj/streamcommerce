"""Deterministic synthetic orders, order items, payments and refunds."""
from __future__ import annotations

import random
from datetime import datetime, timedelta

from src.generators.common import iso

STATUS_WEIGHTS = [("delivered", 55), ("shipped", 15), ("paid", 12), ("placed", 8), ("cancelled", 10)]
PAYMENT_METHODS = ["card", "card", "card", "paypal", "bank_transfer"]
REFUND_REASONS = ["damaged", "not_as_described", "changed_mind", "late_delivery"]


def generate_order_tables(
    customers: list[dict], products: list[dict], orders: int, seed: int, start: datetime, days: int = 90,
) -> dict[str, list[dict]]:
    """Generate related tables.

    Guarantees (when no faults are injected):
    * every order references an existing customer and has 1-4 items of existing products;
    * ``line_total = quantity * unit_price`` and ``order_total = sum(line_total)``;
    * captured payment amount equals ``order_total``;
    * refunds are only created for delivered orders and never exceed the order total.
    """
    if orders < 1:
        raise ValueError("orders must be >= 1")
    rng = random.Random(f"orders-{seed}")
    statuses = [s for s, w in STATUS_WEIGHTS for _ in range(w)]
    stamps = sorted(rng.randint(0, days * 86400 - 1) for _ in range(orders))

    order_rows, item_rows, payment_rows, refund_rows = [], [], [], []
    refund_n = 0
    for n, offset in enumerate(stamps, start=1):
        order_id = f"ORD-{n:07d}"
        order_ts: datetime = start + timedelta(seconds=offset)
        customer = rng.choice(customers)
        status = rng.choice(statuses)
        total = 0.0
        for line in range(1, rng.randint(1, 4) + 1):
            product = rng.choice(products)
            qty = rng.randint(1, 5)
            line_total = round(qty * product["unit_price"], 2)
            total = round(total + line_total, 2)
            item_rows.append({
                "order_item_id": f"OI-{n:07d}-{line}", "order_id": order_id,
                "product_id": product["product_id"], "quantity": qty,
                "unit_price": product["unit_price"], "line_total": line_total,
            })
        order_rows.append({
            "order_id": order_id, "customer_id": customer["customer_id"], "order_ts": iso(order_ts),
            "status": status, "currency": "USD", "order_total": total,
        })
        if status in ("paid", "shipped", "delivered"):
            payment_rows.append({
                "payment_id": f"PAY-{n:07d}", "order_id": order_id, "amount": total,
                "method": rng.choice(PAYMENT_METHODS), "payment_ts": iso(order_ts + timedelta(minutes=rng.randint(1, 30))),
                "payment_status": "captured",
            })
        if status == "delivered" and rng.random() < 0.06:
            refund_n += 1
            amount = total if rng.random() < 0.5 else round(total * rng.uniform(0.1, 0.9), 2)
            refund_rows.append({
                "refund_id": f"REF-{refund_n:06d}", "order_id": order_id, "amount": amount,
                "refund_ts": iso(order_ts + timedelta(days=rng.randint(3, 14))), "reason": rng.choice(REFUND_REASONS),
            })
    return {"orders": order_rows, "order_items": item_rows, "payments": payment_rows, "refunds": refund_rows}
