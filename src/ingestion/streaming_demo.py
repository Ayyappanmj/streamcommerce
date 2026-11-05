"""Streaming demo: generate -> publish -> consume -> summarise, then exit cleanly.

Modes: ``--transport kafka`` (needs a broker) or ``--transport file`` (replays events through the same
validation/dedup/persistence code without a broker).
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from src.common.config import Settings
from src.common.exceptions import StreamCommerceError
from src.common.logging_config import configure_logging
from src.generators.cli import generate_dataset
from src.ingestion.kafka_consumer import run_consumer
from src.ingestion.kafka_producer import produce_lines
from src.processing.event_processor import EventStore, replay_lines


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="StreamCommerce streaming demo")
    p.add_argument("--transport", choices=["kafka", "file"], default="kafka")
    p.add_argument("--customers", type=int, default=100)
    p.add_argument("--products", type=int, default=30)
    p.add_argument("--orders", type=int, default=200)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--faults", action="store_true", help="inject duplicates, malformed and invalid events")
    p.add_argument("--duration", type=float, default=30, help="max consumer run time in seconds")
    p.add_argument("--idle-timeout", type=float, default=10)
    p.add_argument("--rate", type=float, default=None, help="producer events/second (default: as fast as possible)")
    a = p.parse_args(argv)

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    settings.ensure_dirs()
    with tempfile.TemporaryDirectory() as tmp:
        events_file = Path(tmp) / "events.jsonl"
        gen = generate_dataset(Path(tmp) / "csv", a.customers, a.products, a.orders, a.seed, a.faults,
                               events_file=events_file)
        lines = events_file.read_text(encoding="utf-8").splitlines()
    result: dict = {"generated_event_lines": len(lines), "fault_report": gen["fault_report"].get("events", {})}
    try:
        if a.transport == "file":
            result["replay"] = replay_lines(EventStore(settings.events_db_path, settings.late_event_seconds), lines)
        else:
            result["produced"] = produce_lines(settings, lines, events_per_second=a.rate)
            result["consumed"] = run_consumer(settings, a.duration, a.idle_timeout)
    except StreamCommerceError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    store = EventStore(settings.events_db_path, settings.late_event_seconds)
    result["store_counts"] = store.counts()
    result["stream_sales"] = store.stream_sales()
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
