from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from src.batch.warehouse import connect
from src.models.risk import score_event
from src.utils.config import load_settings
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


def _is_late(event: dict[str, Any], watermark_hours: int) -> bool:
    event_ts = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
    ingested = datetime.fromisoformat(event["ingested_at"].replace("Z", "+00:00"))
    # late if processing lag exceeds watermark OR event itself is older than watermark from now
    now = datetime.now(timezone.utc)
    lag = ingested - event_ts
    age = now - event_ts
    return lag > timedelta(hours=watermark_hours) or age > timedelta(hours=watermark_hours * 2)


def process_event(event: dict[str, Any], project_ctx: dict | None = None) -> dict[str, Any]:
    cfg = load_settings()["streaming"]
    risk = score_event(event, project_ctx)
    late = _is_late(event, cfg["watermark_hours"])
    return {
        "event_id": event["event_id"],
        "project_id": event["project_id"],
        "event_ts": event["event_ts"],
        "ingested_at": event["ingested_at"],
        "event_type": event["event_type"],
        "risk_score": risk.score,
        "risk_band": risk.band,
        "is_late": late,
        "model_version": risk.model_version,
        "features_json": json.dumps(risk.features),
    }


def persist_scores(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    con = connect()
    import pandas as pd

    df = pd.DataFrame(rows)
    # idempotent-ish: delete existing event_ids then insert
    ids = df["event_id"].tolist()
    con.executemany("DELETE FROM mart.fact_risk_event WHERE event_id = ?", [[i] for i in ids])
    con.register("_risk", df)
    con.execute("INSERT INTO mart.fact_risk_event SELECT * FROM _risk")
    log.info("Persisted %s risk scores", len(rows))


def consume_kafka(max_messages: int = 50, timeout_s: float = 15.0) -> list[dict[str, Any]]:
    try:
        from confluent_kafka import Consumer, KafkaException
    except ImportError as e:
        raise RuntimeError("confluent-kafka not installed") from e

    cfg = load_settings()["streaming"]
    consumer = Consumer(
        {
            "bootstrap.servers": cfg["bootstrap_servers"],
            "group.id": cfg["consumer_group"],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([cfg["topics"]["sensor_events"]])
    scored: list[dict[str, Any]] = []
    started = datetime.now(timezone.utc)

    try:
        while len(scored) < max_messages:
            if (datetime.now(timezone.utc) - started).total_seconds() > timeout_s:
                break
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("Kafka error: %s", msg.error())
                continue
            event = json.loads(msg.value().decode())
            scored.append(process_event(event))
    finally:
        consumer.close()

    persist_scores(scored)
    return scored
