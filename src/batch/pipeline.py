"""
Batch reliability pipeline.

Steps:
1. Load raw contractor / PM extracts
2. Validate against data contracts → quarantine rejects
3. Normalize regions, compute derived metrics
4. Apply late-arrival window on daily facts
5. Upsert Type-2 dimension for projects
6. Write daily facts with risk scores
7. Record run metadata

Designed to be readable end-to-end. Orchestrators (Airflow, Dagster, cron)
can call `run_pipeline()` without rewriting the logic.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from src.batch.warehouse import connect
from src.contracts.validate import validate_records
from src.models.risk import score_project_snapshot
from src.quality.run_checks import run as run_quality
from src.utils.config import load_settings, path_from_cfg
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


def _row_hash(row: dict) -> str:
    payload = json.dumps(row, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _normalize_projects(df: pd.DataFrame, region_map: pd.DataFrame) -> pd.DataFrame:
    m = region_map.set_index("region_raw")["region_normalized"].to_dict()
    out = df.copy()
    out["region_normalized"] = out["region_raw"].map(m).fillna(out["region_raw"])
    out["planned_start"] = pd.to_datetime(out["planned_start"]).dt.date
    out["planned_end"] = pd.to_datetime(out["planned_end"]).dt.date
    return out


def _dedupe_prefer_freshest(df: pd.DataFrame) -> pd.DataFrame:
    """Same project_id can arrive from pm_export and contractor_csv."""
    df = df.copy()
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)
    df = df.sort_values("extracted_at").drop_duplicates("project_id", keep="last")
    return df


def _scd2_upsert(con, projects: pd.DataFrame, run_ts: datetime) -> int:
    """Simple SCD2 for DuckDB. Close current row when attributes change."""
    changed = 0
    existing = con.execute(
        "SELECT * FROM mart.dim_project WHERE is_current = TRUE"
    ).df()
    existing_by_id = {r["project_id"]: r for _, r in existing.iterrows()} if not existing.empty else {}

    next_sk = int(con.execute("SELECT COALESCE(MAX(project_sk), 0) FROM mart.dim_project").fetchone()[0]) + 1

    for _, row in projects.iterrows():
        attrs = {
            "project_id": row["project_id"],
            "name": row.get("name"),
            "contractor_id": row["contractor_id"],
            "region_normalized": row["region_normalized"],
            "region_raw": row.get("region_raw"),
            "project_type": row.get("project_type"),
            "planned_start": row["planned_start"],
            "planned_end": row["planned_end"],
            "budget_azn": float(row["budget_azn"]),
            "source_system": row.get("source_system"),
        }
        rh = _row_hash(attrs)
        cur = existing_by_id.get(row["project_id"])

        if cur is not None and cur["row_hash"] == rh:
            continue

        if cur is not None:
            con.execute(
                """
                UPDATE mart.dim_project
                SET is_current = FALSE, valid_to = ?
                WHERE project_id = ? AND is_current = TRUE
                """,
                [run_ts, row["project_id"]],
            )

        con.execute(
            """
            INSERT INTO mart.dim_project VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, TRUE, ?)
            """,
            [
                next_sk,
                attrs["project_id"],
                attrs["name"],
                attrs["contractor_id"],
                attrs["region_normalized"],
                attrs["region_raw"],
                attrs["project_type"],
                attrs["planned_start"],
                attrs["planned_end"],
                attrs["budget_azn"],
                attrs["source_system"],
                run_ts,
                rh,
            ],
        )
        next_sk += 1
        changed += 1

    return changed


def _build_daily_facts(
    daily: pd.DataFrame,
    projects: pd.DataFrame,
    late_hours: int,
) -> pd.DataFrame:
    proj = projects.set_index("project_id")
    rows = []
    now = datetime.now(timezone.utc)

    for _, r in daily.iterrows():
        pid = r["project_id"]
        if pid not in proj.index:
            continue
        p = proj.loc[pid]
        if isinstance(p, pd.DataFrame):
            p = p.iloc[0]

        snap = pd.to_datetime(r["snapshot_date"]).date()
        ingested = pd.to_datetime(r["ingested_at"], utc=True)
        late = (ingested - datetime.combine(snap, datetime.min.time(), tzinfo=timezone.utc)) > timedelta(
            hours=late_hours
        )

        planned_start = p["planned_start"]
        planned_end = p["planned_end"]
        if isinstance(planned_start, str):
            planned_start = date.fromisoformat(planned_start)
        if isinstance(planned_end, str):
            planned_end = date.fromisoformat(planned_end)

        total_days = max((planned_end - planned_start).days, 1)
        elapsed = max((snap - planned_start).days, 0)
        planned_elapsed_pct = min(elapsed / total_days, 1.2)

        progress = float(r["progress_pct"]) if pd.notna(r["progress_pct"]) else None
        spent = float(r["spent_azn"]) if pd.notna(r["spent_azn"]) else None
        budget = float(p["budget_azn"])
        cost_over = None if spent is None or budget <= 0 else (spent / budget - 1.0) * 100.0

        expected_progress = planned_elapsed_pct * 100.0
        days_behind = None
        if progress is not None:
            # rough conversion: each missing progress point ≈ total_days / 100
            days_behind = int(max(0, (expected_progress - progress) * total_days / 100.0))

        risk = score_project_snapshot(
            progress_pct=progress,
            planned_elapsed_pct=planned_elapsed_pct,
            quality_score=float(r["quality_score"]) if pd.notna(r.get("quality_score")) else None,
            cost_overrun_pct=cost_over,
        )

        rows.append(
            {
                "project_id": pid,
                "snapshot_date": snap,
                "progress_pct": progress,
                "spent_azn": spent,
                "quality_score": float(r["quality_score"]) if pd.notna(r.get("quality_score")) else None,
                "days_behind": days_behind,
                "cost_overrun_pct": cost_over,
                "risk_score": risk.score,
                "risk_band": risk.band,
                "late_arrival_flag": bool(late),
            }
        )

    return pd.DataFrame(rows)


def run_pipeline() -> dict:
    cfg = load_settings()
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    raw = path_from_cfg("raw")
    staging = path_from_cfg("staging")
    processed = path_from_cfg("processed")

    log.info("Run %s starting", run_id)

    # --- quality gate (soft or hard depending on config) ---
    quality_ok = run_quality()
    if not quality_ok and not cfg["batch"]["quarantine_on_fail"]:
        raise RuntimeError("Quality gate failed and quarantine_on_fail is false")

    projects_raw = pd.read_csv(raw / "projects.csv")
    records = projects_raw.to_dict(orient="records")
    # jsonschema date-time likes strings; csv already has them
    valid, rejected = validate_records(records, "projects_v1.json")

    if rejected:
        rej_df = pd.DataFrame(rejected)
        rej_path = staging / f"quarantine_projects_{run_id[:8]}.csv"
        rej_df.to_csv(rej_path, index=False)
        log.warning("Quarantined %s project rows → %s", len(rejected), rej_path)

    projects = pd.DataFrame(valid)
    region_map = pd.read_csv(path_from_cfg("reference") / "region_map.csv")
    projects = _normalize_projects(projects, region_map)
    projects = _dedupe_prefer_freshest(projects)

    # drop remaining non-positive budgets after contract (defence in depth)
    before = len(projects)
    projects = projects[projects["budget_azn"] > 0].copy()
    if len(projects) < before:
        log.warning("Dropped %s rows with non-positive budget post-contract", before - len(projects))

    daily_raw = pd.read_csv(raw / "project_daily.csv")
    daily_facts = _build_daily_facts(
        daily_raw,
        projects,
        late_hours=cfg["batch"]["late_arrival_hours"],
    )

    con = connect()
    dim_changes = _scd2_upsert(con, projects, started)

    # replace daily facts for overlapping keys (idempotent enough for demo)
    if not daily_facts.empty:
        con.execute("DELETE FROM mart.fact_project_daily WHERE snapshot_date >= ?", [daily_facts["snapshot_date"].min()])
        con.register("_daily", daily_facts)
        con.execute("INSERT INTO mart.fact_project_daily SELECT * FROM _daily")

    finished = datetime.now(timezone.utc)
    con.execute(
        """
        INSERT INTO meta.pipeline_run VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            "batch_reliability",
            started,
            finished,
            "success",
            len(records),
            len(projects),
            len(rejected),
            f"dim_changes={dim_changes}, daily_rows={len(daily_facts)}, quality_ok={quality_ok}",
        ],
    )

    # also dump parquet for people who just want files
    projects.to_parquet(processed / "projects_clean.parquet", index=False)
    daily_facts.to_parquet(processed / "fact_project_daily.parquet", index=False)

    summary = {
        "run_id": run_id,
        "rows_in": len(records),
        "rows_out": len(projects),
        "quarantined": len(rejected),
        "dim_changes": dim_changes,
        "daily_facts": len(daily_facts),
        "late_flagged": int(daily_facts["late_arrival_flag"].sum()) if not daily_facts.empty else 0,
    }
    log.info("Pipeline finished: %s", summary)
    (processed / f"run_{run_id[:8]}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    # ensure sample data exists
    raw = path_from_cfg("raw")
    if not (raw / "projects.csv").exists():
        from src.batch.generate_sample_data import main as gen

        gen()
    run_pipeline()


if __name__ == "__main__":
    main()
