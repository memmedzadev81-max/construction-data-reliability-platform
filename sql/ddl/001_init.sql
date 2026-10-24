-- Applied automatically on first Postgres start (docker-entrypoint-initdb.d).
-- Keep DuckDB path in sync via src/batch/warehouse.py for local runs without Docker.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;

-- Slowly changing dimension for projects (Type 2)
CREATE TABLE IF NOT EXISTS mart.dim_project (
    project_sk        BIGSERIAL PRIMARY KEY,
    project_id        TEXT NOT NULL,
    name              TEXT,
    contractor_id     TEXT,
    region_normalized TEXT,
    region_raw        TEXT,
    project_type      TEXT,
    planned_start     DATE,
    planned_end       DATE,
    budget_azn        NUMERIC(18,2),
    source_system     TEXT,
    valid_from        TIMESTAMPTZ NOT NULL,
    valid_to          TIMESTAMPTZ,
    is_current        BOOLEAN NOT NULL DEFAULT TRUE,
    row_hash          TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_project_current
    ON mart.dim_project (project_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS mart.fact_project_daily (
    project_id        TEXT NOT NULL,
    snapshot_date     DATE NOT NULL,
    progress_pct      NUMERIC(6,2),
    spent_azn         NUMERIC(18,2),
    quality_score     NUMERIC(6,2),
    days_behind       INTEGER,
    cost_overrun_pct  NUMERIC(8,2),
    risk_score        NUMERIC(6,4),
    risk_band         TEXT,
    late_arrival_flag BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (project_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS mart.fact_risk_event (
    event_id          TEXT PRIMARY KEY,
    project_id        TEXT NOT NULL,
    event_ts          TIMESTAMPTZ NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL,
    event_type        TEXT,
    risk_score        NUMERIC(6,4),
    risk_band         TEXT,
    is_late           BOOLEAN,
    model_version     TEXT,
    features_json     JSONB
);

CREATE TABLE IF NOT EXISTS meta.pipeline_run (
    run_id            TEXT PRIMARY KEY,
    pipeline_name     TEXT NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    status            TEXT,
    rows_in           BIGINT,
    rows_out          BIGINT,
    rows_quarantined  BIGINT,
    notes             TEXT
);

CREATE TABLE IF NOT EXISTS meta.quality_result (
    check_id          BIGSERIAL PRIMARY KEY,
    run_id            TEXT,
    dataset           TEXT,
    check_name        TEXT,
    severity          TEXT,
    passed            BOOLEAN,
    observed_value    TEXT,
    threshold         TEXT,
    checked_at        TIMESTAMPTZ DEFAULT NOW()
);
