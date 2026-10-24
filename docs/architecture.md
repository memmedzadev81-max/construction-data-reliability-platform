# Architecture

## Why this shape

Road construction data does not behave like clean SaaS events. Exports arrive late, region names disagree, the same project shows up from two systems with different progress numbers, and a zero budget in a CSV will silently destroy every overrun metric downstream.

The platform is split into two paths that share the same risk model and warehouse:

```
                    ┌─────────────────────┐
   contractor CSV ─►│  Batch reliability  │──► mart.dim_project (SCD2)
   PM export      ─►│  contracts + DQ     │──► mart.fact_project_daily
   field packs    ─►│  quarantine         │
                    └─────────────────────┘

   site sensors ───► Kafka / Redpanda ──► risk consumer ──► mart.fact_risk_event
                         │
                         └── offline JSONL fallback (no broker required)
```

## Batch path

1. **Contracts** (`config/contracts`) — JSON Schema. Rejected rows go to quarantine files, not into dims.
2. **Quality gates** — cheap checks that fail the run or force quarantine depending on `settings.yaml`.
3. **Normalization** — region map, freshest-wins dedupe on `project_id`.
4. **SCD2** — attribute changes close the current `dim_project` row.
5. **Daily facts** — progress, overrun, days behind, risk score, late-arrival flag.

Default warehouse is **DuckDB** so a clone works without Docker. Postgres DDL is in `sql/ddl` for when you flip the engine.

## Streaming path

Events carry both `event_ts` (business time) and `ingested_at` (arrival time). The consumer flags late data against a watermark instead of dropping it. Dropping late construction events is usually the wrong call — the site still happened.

Risk scoring is the same function used in batch so a daily snapshot and a live weather spike stay comparable.

## What is intentionally missing

- Full Airflow/Dagster graphs — the logic is callable; wire your orchestrator later.
- Multi-cluster Kafka auth — local Redpanda only.
- ML training loop — the score is rule-based on purpose. Explainability > leaderboard points for this domain.
