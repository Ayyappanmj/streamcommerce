"""Optional fault injection for CSV tables. Disabled unless explicitly requested."""
from __future__ import annotations

import copy
import random

from src.common.schemas import DATASETS


def _pick(rng: random.Random, n: int, rate: float) -> list[int]:
    if n == 0:
        return []
    return rng.sample(range(n), min(n, max(1, int(n * rate))))


def inject_table_faults(
    tables: dict[str, list[dict]], seed: int, rate: float = 0.02,
) -> tuple[dict[str, list[dict]], dict[str, list[str]], dict[str, int]]:
    """Return faulty copies of ``tables``, raw malformed lines per dataset, and a report of counts.

    Faults: duplicate orders, negative order totals, blank customer IDs,
    unparseable timestamps, and structurally malformed CSV lines.
    """
    rng = random.Random(f"faults-{seed}")
    out = copy.deepcopy(tables)
    report = {"duplicate_orders": 0, "invalid_order_amounts": 0, "missing_customer_ids": 0,
              "bad_timestamps": 0, "malformed_lines": 0}
    orders = out["orders"]
    for i in _pick(rng, len(orders), rate):
        orders[i]["order_total"] = -abs(orders[i]["order_total"]) - 1
        report["invalid_order_amounts"] += 1
    for i in _pick(rng, len(orders), rate):
        orders[i]["customer_id"] = ""
        report["missing_customer_ids"] += 1
    for i in _pick(rng, len(orders), rate):
        orders[i]["order_ts"] = "not-a-timestamp"
        report["bad_timestamps"] += 1
    duplicates = [copy.deepcopy(orders[i]) for i in _pick(rng, len(orders), rate)]
    orders.extend(duplicates)
    report["duplicate_orders"] = len(duplicates)

    raw_lines: dict[str, list[str]] = {}
    width = len(DATASETS["orders"].columns)
    raw_lines["orders"] = [",".join(["BAD-ROW"] + ["x"] * (width + 1))]
    report["malformed_lines"] = 1
    return out, raw_lines, report
