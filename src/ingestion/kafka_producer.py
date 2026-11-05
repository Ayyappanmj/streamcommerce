"""Publish JSON order events to Kafka/Redpanda (confluent-kafka)."""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable

from src.common.config import Settings
from src.common.exceptions import IngestionError, MissingDependencyError

log = logging.getLogger(__name__)


def _key_for(line: str) -> bytes | None:
    """Partition by order_id so events of one order stay ordered; malformed lines get no key."""
    try:
        return str(json.loads(line)["order_id"]).encode()
    except (ValueError, KeyError, TypeError):
        return None


def produce_lines(settings: Settings, lines: Iterable[str], topic: str | None = None,
                  events_per_second: float | None = None) -> dict[str, int]:
    """Send each line as one message. Raises :class:`IngestionError` if any delivery failed."""
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise MissingDependencyError("confluent-kafka is not installed (pip install confluent-kafka)") from exc
    topic = topic or settings.kafka_topic
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap, "acks": "all", "enable.idempotence": True,
                         "client.id": "streamcommerce-producer"})
    stats = {"sent": 0, "failed": 0}

    def on_delivery(err, _msg) -> None:
        if err is not None:
            stats["failed"] += 1
            log.error("delivery failed", extra={"error": str(err)})

    for line in lines:
        if not line.strip():
            continue
        producer.produce(topic, value=line.encode("utf-8"), key=_key_for(line), on_delivery=on_delivery)
        stats["sent"] += 1
        producer.poll(0)
        if events_per_second:
            time.sleep(1.0 / events_per_second)
    remaining = producer.flush(30)
    if remaining or stats["failed"]:
        raise IngestionError(f"{remaining} message(s) undelivered, {stats['failed']} delivery error(s)")
    return stats
