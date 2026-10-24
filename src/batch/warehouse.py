from __future__ import annotations

from pathlib import Path

import duckdb

from src.utils.config import load_settings, path_from_cfg
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


def connect():
    cfg = load_settings()
    engine = cfg["warehouse"]["engine"]
    if engine == "duckdb":
        path = Path(cfg["warehouse"]["duckdb_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(path))
        con.execute("CREATE SCHEMA IF NOT EXISTS mart")
        con.execute("CREATE SCHEMA IF NOT EXISTS meta")
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        _ensure_duckdb_tables(con)
        return con
    raise NotImplementedError(
        "Postgres path is prepared in sql/ddl; set WAREHOUSE_ENGINE=duckdb for local default."
    )


def _ensure_duckdb_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mart.dim_project (
            project_sk BIGINT,
            project_id VARCHAR,
            name VARCHAR,
            contractor_id VARCHAR,
            region_normalized VARCHAR,
            region_raw VARCHAR,
            project_type VARCHAR,
            planned_start DATE,
            planned_end DATE,
            budget_azn DOUBLE,
            source_system VARCHAR,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN,
            row_hash VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mart.fact_project_daily (
            project_id VARCHAR,
            snapshot_date DATE,
            progress_pct DOUBLE,
            spent_azn DOUBLE,
            quality_score DOUBLE,
            days_behind INTEGER,
            cost_overrun_pct DOUBLE,
            risk_score DOUBLE,
            risk_band VARCHAR,
            late_arrival_flag BOOLEAN
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mart.fact_risk_event (
            event_id VARCHAR,
            project_id VARCHAR,
            event_ts TIMESTAMP,
            ingested_at TIMESTAMP,
            event_type VARCHAR,
            risk_score DOUBLE,
            risk_band VARCHAR,
            is_late BOOLEAN,
            model_version VARCHAR,
            features_json VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.pipeline_run (
            run_id VARCHAR,
            pipeline_name VARCHAR,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR,
            rows_in BIGINT,
            rows_out BIGINT,
            rows_quarantined BIGINT,
            notes VARCHAR
        )
        """
    )
