"""Deterministic synthetic customers (no real personal data)."""
from __future__ import annotations

import random
from datetime import datetime, timedelta

from src.generators.common import iso

COUNTRIES = ["US", "CA", "GB", "DE", "FR", "IN", "AU", "BR", "JP", "NL"]
SEGMENTS = ["consumer", "consumer", "consumer", "small_business", "enterprise"]


def generate_customers(count: int, seed: int, start: datetime) -> list[dict]:
    """Return ``count`` customers. Emails use the reserved example.com domain."""
    if count < 1:
        raise ValueError("count must be >= 1")
    rng = random.Random(f"customers-{seed}")
    rows = []
    for i in range(1, count + 1):
        signup = start - timedelta(days=rng.randint(1, 365), seconds=rng.randint(0, 86399))
        rows.append({
            "customer_id": f"C{i:06d}",
            "email": f"customer{i}@example.com",
            "country": rng.choice(COUNTRIES),
            "segment": rng.choice(SEGMENTS),
            "signup_ts": iso(signup),
        })
    return rows
