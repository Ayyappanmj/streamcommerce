"""Command-line entry point: ``python -m src.generators.cli --orders 10000 --seed 42``."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.common.config import PROJECT_ROOT
from src.common.schemas import DATASETS
from src.generators.common import DEFAULT_START_DATE, parse_start_date, write_csv
from src.generators.faults import inject_table_faults
from src.generators.generate_customers import generate_customers
from src.generators.generate_events import build_events, inject_event_faults, to_wire
from src.generators.generate_orders import generate_order_tables
from src.generators.generate_products import generate_products


def generate_dataset(
    output_dir: Path, customers: int = 1000, products: int = 200, orders: int = 10000, seed: int = 42,
    faults: bool = False, fault_rate: float = 0.02, start_date: str = DEFAULT_START_DATE, days: int = 90,
    events_file: Path | None = None,
) -> dict:
    """Generate CSV files (and optionally a JSONL event file). Returns a summary dict."""
    if not 0 < fault_rate < 1:
        raise ValueError("fault_rate must be between 0 and 1 (exclusive)")
    start = parse_start_date(start_date)
    cust = generate_customers(customers, seed, start)
    prod = generate_products(products, seed, start)
    tables = {"customers": cust, "products": prod, **generate_order_tables(cust, prod, orders, seed, start, days)}
    clean_events = build_events(tables, seed)

    raw_lines: dict[str, list[str]] = {}
    fault_report: dict[str, dict] = {}
    csv_tables = tables
    if faults:
        csv_tables, raw_lines, fault_report["tables"] = inject_table_faults(tables, seed, fault_rate)
    for name, spec in DATASETS.items():
        write_csv(output_dir / f"{name}.csv", spec.column_names, csv_tables[name], raw_lines.get(name))

    summary: dict = {"seed": seed, "faults": faults, "counts": {k: len(v) for k, v in csv_tables.items()},
                     "output_dir": str(output_dir), "fault_report": fault_report}
    if events_file is not None:
        if faults:
            lines, fault_report["events"] = inject_event_faults(clean_events, seed, fault_rate)
        else:
            lines = [to_wire(e) for e in clean_events]
        events_file.parent.mkdir(parents=True, exist_ok=True)
        events_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        summary["events_file"] = str(events_file)
        summary["counts"]["event_lines"] = len(lines)
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Generate deterministic synthetic e-commerce data.")
    p.add_argument("--customers", type=int, default=1000)
    p.add_argument("--products", type=int, default=200)
    p.add_argument("--orders", type=int, default=10000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--faults", action="store_true", help="inject faults (off by default)")
    p.add_argument("--fault-rate", type=float, default=0.02)
    p.add_argument("--start-date", default=DEFAULT_START_DATE)
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "data" / "raw" / "incoming")
    p.add_argument("--events-file", type=Path, default=None, help="also write events as JSON lines")
    a = p.parse_args(argv)
    summary = generate_dataset(a.output_dir, a.customers, a.products, a.orders, a.seed, a.faults,
                               a.fault_rate, a.start_date, a.days, a.events_file)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
