"""Needs a running broker: set KAFKA_TEST_BOOTSTRAP=localhost:9092 (docker compose --profile streaming up -d)."""
import os
import uuid

import pytest

from src.generators.cli import generate_dataset
from tests.helpers import make_settings

pytestmark = pytest.mark.external
pytest.importorskip("confluent_kafka")
BOOTSTRAP = os.environ.get("KAFKA_TEST_BOOTSTRAP")


@pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_TEST_BOOTSTRAP not set")
def test_produce_consume_roundtrip_with_duplicates_and_invalid_events(tmp_path):
    from src.ingestion.kafka_consumer import run_consumer
    from src.ingestion.kafka_producer import produce_lines
    from src.processing.event_processor import EventStore
    suffix = uuid.uuid4().hex[:6]
    s = make_settings(tmp_path, kafka_bootstrap=BOOTSTRAP, kafka_topic=f"test.events.{suffix}",
                      kafka_dlq_topic=f"test.dlq.{suffix}", kafka_group_id=f"test-{suffix}")
    generate_dataset(tmp_path / "csv", 20, 8, 40, 5, faults=True, events_file=tmp_path / "e.jsonl")
    lines = (tmp_path / "e.jsonl").read_text().splitlines()
    assert produce_lines(s, lines)["failed"] == 0
    run_consumer(s, duration_seconds=60, idle_timeout_seconds=8)
    before = EventStore(s.events_db_path).stream_sales()
    run_consumer(s, duration_seconds=10, idle_timeout_seconds=3)  # same group: nothing left to read
    counts = EventStore(s.events_db_path).counts()
    assert counts["processed"] > 0 and counts["quarantined"] >= 1 and counts["duplicates"] >= 1
    assert EventStore(s.events_db_path).stream_sales() == before
