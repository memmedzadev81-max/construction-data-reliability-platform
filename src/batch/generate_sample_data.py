"""
Generate realistic-but-dirty sample data for local demos.

Why dirty on purpose:
- Contractor exports rarely match internal coding.
- Region names come in three spellings for the same place.
- Progress packs arrive late; some quality scores are missing.
- A few duplicate project_ids appear across source systems.

This is not production volume. It is enough to exercise contracts,
quarantine, SCD2 and late-arrival logic.
"""
from __future__ import annotations

import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from src.utils.config import path_from_cfg
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

RNG = random.Random(42)

CONTRACTORS = [f"C{str(i).zfill(3)}" for i in range(1, 9)]
REGIONS_RAW = [
    "Baku",
    "Bakı",
    "BAKU",
    "Ganja",
    "Gəncə",
    "Sumqayit",
    "Sumqayıt",
    "Mingachevir",
    "Lankaran",
    "Şəki",
    "Sheki",
]
PROJECT_TYPES = ["road", "highway", "bridge", "tunnel"]
SOURCES = ["pm_export", "contractor_csv", "field_app"]


def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=RNG.randint(0, max(delta, 1)))


def build_projects(n: int = 120) -> pd.DataFrame:
    rows = []
    base = date(2023, 1, 1)
    today = date(2026, 10, 1)

    for i in range(1, n + 1):
        pid = f"RP-{1000 + i}"
        planned_start = _rand_date(base, date(2025, 6, 1))
        duration = RNG.randint(90, 540)
        planned_end = planned_start + timedelta(days=duration)
        started = planned_start + timedelta(days=RNG.randint(-10, 40))

        # ~35% finished, rest ongoing
        finished = RNG.random() < 0.35 and planned_end < today
        if finished:
            slip = RNG.randint(-20, 120)
            actual_end = planned_end + timedelta(days=slip)
            progress = 100.0
        else:
            actual_end = None
            # progress can overshoot 100 in bad exports
            progress = round(min(118.0, RNG.uniform(5, 105)), 1)

        budget = round(RNG.uniform(0.8, 45.0) * 1_000_000, 2)
        spent_ratio = RNG.uniform(0.4, 1.35)
        spent = round(budget * spent_ratio * (progress / 100.0), 2)

        # Missing quality on purpose for ~12% of rows
        quality = None if RNG.random() < 0.12 else round(RNG.uniform(55, 98), 1)

        rows.append(
            {
                "project_id": pid,
                "name": f"Section {i} — {RNG.choice(['ring', 'access', 'bypass', 'link'])}",
                "contractor_id": RNG.choice(CONTRACTORS),
                "region_raw": RNG.choice(REGIONS_RAW),
                "project_type": RNG.choice(PROJECT_TYPES),
                "planned_start": planned_start.isoformat(),
                "planned_end": planned_end.isoformat(),
                "actual_start": started.isoformat(),
                "actual_end": actual_end.isoformat() if actual_end else None,
                "budget_azn": budget,
                "spent_azn": spent,
                "progress_pct": progress,
                "quality_score": quality,
                "source_system": RNG.choice(SOURCES),
                "extracted_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Inject a few intentional problems reviewers actually see on site data
    # 1) Duplicate project from second source
    dup = df.iloc[3].copy()
    dup["source_system"] = "contractor_csv"
    dup["progress_pct"] = float(dup["progress_pct"]) + 4.0
    dup["extracted_at"] = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    df = pd.concat([df, pd.DataFrame([dup])], ignore_index=True)

    # 2) Broken budget (zero) — should fail contract / quality
    df.loc[df.index[7], "budget_azn"] = 0.0

    # 3) Future planned_start typo
    df.loc[df.index[11], "planned_start"] = "2028-01-01"

    return df


def build_daily_progress(projects: pd.DataFrame, days: int = 45) -> pd.DataFrame:
    """Daily snapshots with some late arrivals."""
    rows = []
    today = date(2026, 10, 20)
    active = projects[projects["actual_end"].isna()].head(40)

    for _, p in active.iterrows():
        for d in range(days):
            snap = today - timedelta(days=days - d)
            # late arrival: some rows "arrive" 2–4 days after snapshot_date
            lag = RNG.choice([0, 0, 0, 1, 2, 3, 4])
            ingested = datetime.combine(snap, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
                days=lag, hours=RNG.randint(6, 20)
            )
            rows.append(
                {
                    "project_id": p["project_id"],
                    "snapshot_date": snap.isoformat(),
                    "progress_pct": max(0.0, float(p["progress_pct"]) - (days - d) * RNG.uniform(0.1, 0.8)),
                    "spent_azn": float(p["spent_azn"]) * (0.7 + 0.3 * d / days),
                    "quality_score": p["quality_score"],
                    "ingested_at": ingested.isoformat(),
                    "source_system": p["source_system"],
                }
            )
    return pd.DataFrame(rows)


def build_sensor_seed(projects: pd.DataFrame, n_events: int = 400) -> list[dict]:
    """Seed events for the streaming demo (also written as JSONL)."""
    event_types = ["progress_update", "weather", "equipment_stop", "quality_check", "material_delay"]
    pids = projects["project_id"].tolist()
    events = []
    now = datetime.now(timezone.utc)

    for i in range(n_events):
        et = RNG.choice(event_types)
        event_ts = now - timedelta(hours=RNG.randint(0, 72), minutes=RNG.randint(0, 59))
        # some events are late relative to processing time
        ingested = event_ts + timedelta(minutes=RNG.randint(1, 180))
        payload: dict = {}
        if et == "weather":
            payload = {"rain_mm": round(RNG.uniform(0, 45), 1), "wind_ms": round(RNG.uniform(0, 18), 1)}
        elif et == "progress_update":
            payload = {"reported_pct": round(RNG.uniform(10, 100), 1)}
        elif et == "equipment_stop":
            payload = {"hours": round(RNG.uniform(0.5, 12), 1), "reason": RNG.choice(["breakdown", "fuel", "access"])}
        elif et == "quality_check":
            payload = {"score": round(RNG.uniform(50, 99), 1)}
        else:
            payload = {"days_impact": RNG.randint(1, 10), "material": RNG.choice(["asphalt", "steel", "concrete"])}

        events.append(
            {
                "event_id": f"EVT-{10000 + i}",
                "project_id": RNG.choice(pids),
                "event_ts": event_ts.isoformat(),
                "ingested_at": ingested.isoformat(),
                "event_type": et,
                "payload": payload,
            }
        )
    return events


def main() -> None:
    raw = path_from_cfg("raw")
    log.info("Writing sample data under %s", raw)

    projects = build_projects(120)
    projects.to_csv(raw / "projects.csv", index=False)

    daily = build_daily_progress(projects)
    daily.to_csv(raw / "project_daily.csv", index=False)

    events = build_sensor_seed(projects)
    with (raw / "sensor_events.jsonl").open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    # Reference map for region normalization
    ref = path_from_cfg("reference")
    region_map = pd.DataFrame(
        [
            {"region_raw": "Baku", "region_normalized": "Baku"},
            {"region_raw": "Bakı", "region_normalized": "Baku"},
            {"region_raw": "BAKU", "region_normalized": "Baku"},
            {"region_raw": "Ganja", "region_normalized": "Ganja"},
            {"region_raw": "Gəncə", "region_normalized": "Ganja"},
            {"region_raw": "Sumqayit", "region_normalized": "Sumqayit"},
            {"region_raw": "Sumqayıt", "region_normalized": "Sumqayit"},
            {"region_raw": "Mingachevir", "region_normalized": "Mingachevir"},
            {"region_raw": "Lankaran", "region_normalized": "Lankaran"},
            {"region_raw": "Şəki", "region_normalized": "Sheki"},
            {"region_raw": "Sheki", "region_normalized": "Sheki"},
        ]
    )
    region_map.to_csv(ref / "region_map.csv", index=False)

    log.info(
        "Done: %s projects, %s daily rows, %s sensor events",
        len(projects),
        len(daily),
        len(events),
    )


if __name__ == "__main__":
    main()
