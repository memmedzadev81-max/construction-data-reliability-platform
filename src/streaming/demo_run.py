"""
Streaming demo with automatic offline fallback.

If Redpanda is not running, we still score the JSONL seed file so reviewers
can see the risk path without Docker. When Kafka is up, we publish + consume.
"""
from __future__ import annotations

from src.streaming.consumer import persist_scores, process_event
from src.streaming.producer import iter_events, publish_to_kafka
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


def _kafka_available() -> bool:
    try:
        from confluent_kafka import Producer

        from src.utils.config import load_settings

        p = Producer({"bootstrap.servers": load_settings()["streaming"]["bootstrap_servers"]})
        # list topics with a short timeout — fails fast if broker down
        p.list_topics(timeout=3)
        return True
    except Exception as e:
        log.warning("Kafka not available (%s) — using offline path", e)
        return False


def offline_score(limit: int = 80) -> list[dict]:
    rows = [process_event(e) for e in iter_events(limit=limit)]
    persist_scores(rows)
    high = sum(1 for r in rows if r["risk_band"] == "high")
    late = sum(1 for r in rows if r["is_late"])
    log.info("Offline scored %s events | high_risk=%s late=%s", len(rows), high, late)
    return rows


def main() -> None:
    if _kafka_available():
        published = publish_to_kafka(limit=60)
        from src.streaming.consumer import consume_kafka

        scored = consume_kafka(max_messages=published, timeout_s=20)
        log.info("Kafka path done: published=%s scored=%s", published, len(scored))
    else:
        offline_score()


if __name__ == "__main__":
    main()
