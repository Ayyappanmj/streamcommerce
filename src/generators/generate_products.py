"""Deterministic synthetic product catalogue."""
from __future__ import annotations

import random
from datetime import datetime, timedelta

from src.generators.common import iso

CATEGORIES = {
    "electronics": (15.0, 400.0), "home": (5.0, 150.0), "books": (4.0, 40.0),
    "clothing": (8.0, 120.0), "sports": (10.0, 200.0), "beauty": (3.0, 60.0),
}
ADJECTIVES = ["Compact", "Classic", "Deluxe", "Eco", "Smart", "Portable", "Premium", "Basic"]


def generate_products(count: int, seed: int, start: datetime) -> list[dict]:
    """Return ``count`` products with prices rounded to cents."""
    if count < 1:
        raise ValueError("count must be >= 1")
    rng = random.Random(f"products-{seed}")
    categories = sorted(CATEGORIES)
    rows = []
    for i in range(1, count + 1):
        category = rng.choice(categories)
        low, high = CATEGORIES[category]
        rows.append({
            "product_id": f"P{i:05d}",
            "product_name": f"{rng.choice(ADJECTIVES)} {category.title()} Item {i}",
            "category": category,
            "unit_price": round(rng.uniform(low, high), 2),
            "created_ts": iso(start - timedelta(days=rng.randint(30, 700))),
        })
    return rows
