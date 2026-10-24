from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterator

from src.utils.config import load_settings, path_from_cfg
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


def iter_events(limit: int | None = None) -> Iterator[dict]:
    path = path_from_cfg("raw") / "sensor_events.jsonl"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}; run: make raw")
    count = 0
    with path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            yield json.loads(line)
            count += 1
            if limit is not None and count >= limit:
                break


def publish_to_kafka(limit: int = 50, sleep_s: float = 0.05) -> int:
    """Publish seed events to Redpanda/Kafka. Returns count sent."""
    try:
        from confluent_kafka import Producer
    except ImportError as e:
        raise RuntimeError("confluent-kafka not installed") from e

    cfg = load_settings()["streaming"]
    producer = Producer({"bootstrap.servers": cfg["bootstrap_servers"]})
    topic = cfg["topics"]["sensor_events"]
    sent = 0

    def _cb(err, msg):
        if err:
            log.error("Produce failed: %s", err)

    for event in iter_events(limit=limit):
        producer.produce(
            topic,
            key=event["project_id"].encode(),
            value=json.dumps(event).encode(),
            callback=_cb,
        )
        sent += 1
        if sent % 10 == 0:
            producer.poll(0)
        time.sleep(sleep_s)

    producer.flush(10)
    log.info("Published %s events to %s", sent, topic)
    return sent
