"""Kafka/Redpanda consumer with manual offset commits and graceful shutdown.

Offset policy (deliberate): ``enable.auto.commit=False``. An offset is committed only AFTER the message was
durably stored (processed, recognised as duplicate, or quarantined). A crash before the commit causes
re-delivery, which the idempotent store absorbs. This yields at-least-once delivery with effectively-once results.
"""
from __future__ import annotations

import logging
import signal
import threading
import time

from src.common.config import Settings
from src.common.exceptions import IngestionError, MissingDependencyError
from src.processing.event_processor import QUARANTINED, EventStore

log = logging.getLogger(__name__)


def run_consumer(settings: Settings, duration_seconds: float = 30, idle_timeout_seconds: float = 10,
                 max_messages: int | None = None, stop_event: threading.Event | None = None) -> dict[str, int]:
    """Consume until duration elapses, the stream is idle, ``max_messages`` is reached, or SIGINT/SIGTERM."""
    try:
        from confluent_kafka import Consumer, KafkaError, Producer
    except ImportError as exc:
        raise MissingDependencyError("confluent-kafka is not installed (pip install confluent-kafka)") from exc

    stop = stop_event or threading.Event()
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stop.set())

    store = EventStore(settings.events_db_path, settings.late_event_seconds)
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap, "group.id": settings.kafka_group_id,
        "enable.auto.commit": False, "auto.offset.reset": "earliest", "client.id": "streamcommerce-consumer"})
    dlq = Producer({"bootstrap.servers": settings.kafka_bootstrap, "client.id": "streamcommerce-dlq"})
    consumer.subscribe([settings.kafka_topic])
    tally = {"processed": 0, "duplicate": 0, "quarantined": 0, "consumed": 0}
    started = last_msg = time.monotonic()
    try:
        while not stop.is_set():
            now = time.monotonic()
            if now - started >= duration_seconds or now - last_msg >= idle_timeout_seconds:
                break
            if max_messages is not None and tally["consumed"] >= max_messages:
                break
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise IngestionError(f"Kafka consumer error: {msg.error()}")
            last_msg = time.monotonic()
            source = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
            outcome = store.process(msg.value() or b"", source=source)  # DB error -> exception -> no commit
            tally[outcome] += 1
            tally["consumed"] += 1
            if outcome == QUARANTINED:
                dlq.produce(settings.kafka_dlq_topic, value=msg.value(), key=msg.key())
                dlq.poll(0)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        log.info("consumer shutting down", extra=tally)
        dlq.flush(10)
        consumer.close()
    return tally
