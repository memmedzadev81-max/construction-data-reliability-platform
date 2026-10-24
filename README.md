# Construction Data Reliability Platform

Multi-source reliability pipeline + near real-time delay risk for road / highway / bridge programmes.

This is not another “ingest CSV → Postgres → dashboard” tutorial. The design assumes the mess that actually shows up on infrastructure projects: conflicting region spellings, duplicate project ids from contractor vs PM systems, missing quality scores, late daily packs, and the need for a risk number a site engineer can argue with.

I work with construction-side data (see also [road-project-delay-analysis](https://github.com/memmedzadev81-max/road-project-delay-analysis) and [road-delay-risk-ml](https://github.com/memmedzadev81-max/road-delay-risk-ml)). This repo is the engineering layer those analyses need underneath them.

---

## What you get

| Area | Capability |
|------|------------|
| Contracts | JSON Schema for projects & sensor events; rejects → quarantine files |
| Quality gates | Critical + warning checks; configurable pass threshold |
| Batch pipeline | Normalize → dedupe → SCD2 dim → daily facts with late-arrival flags |
| Risk model | Explainable weighted score (weights in config, feature breakdown returned) |
| Streaming | Redpanda/Kafka path + **offline fallback** on the same seed events |
| Serving | Small FastAPI surface for snapshot/event scoring |
| Local warehouse | DuckDB by default; Postgres DDL included |

---

## Quick start

```bash
git clone https://github.com/memmedzadev81-max/construction-data-reliability-platform.git
cd construction-data-reliability-platform

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# sample dirty data
make raw

# quality report
make quality

# full batch pipeline (DuckDB warehouse under data/warehouse/)
make batch

# streaming demo (works without Docker via offline path)
make stream
```

Optional infrastructure:

```bash
make up      # Postgres 16 + Redpanda
make stream  # will publish/consume if broker is healthy
make risk    # API on http://localhost:8080/docs
make down
```

---

## Layout

```
config/           settings + data contracts
data/raw          generated sample extracts (gitignored outputs under processed/)
docs/             architecture + decision log
sql/ddl           Postgres bootstrap
src/batch         sample generator + reliability pipeline
src/contracts     schema validation
src/models        explainable risk scoring
src/quality       gates
src/streaming     producer, consumer, API, offline demo
tests/
```

---

## Design choices (short)

- **Quarantine by default** — one bad budget row should not kill the daily load for the whole programme.
- **SCD2 on projects** — contractor, region and plan dates change; history matters for audit.
- **Late data is flagged, not dropped** — construction events still happened.
- **Same risk function in batch and stream** — comparable scores.
- **DuckDB first** — clone and run without waiting on containers; Postgres when you need it.

Longer notes: [docs/architecture.md](docs/architecture.md), [docs/decisions.md](docs/decisions.md).

---

## Sample results you should see

After `make raw && make batch`:

- Quarantine file under `data/staging/` for contract failures (zero budget, etc.)
- `data/processed/projects_clean.parquet` and `fact_project_daily.parquet`
- DuckDB tables `mart.dim_project`, `mart.fact_project_daily`, `meta.pipeline_run`

After `make stream`:

- Rows in `mart.fact_risk_event` with `risk_band` and `is_late`

---

## Tests

```bash
pytest -q tests/
```

---

## Limits (honest)

- Sample volume is demo-sized (~120 projects, a few hundred events).
- Risk weights are domain heuristics, not fitted coefficients.
- Kafka security, multi-tenant isolation and full orchestrator DAGs are out of scope on purpose.

If you are adapting this to a live programme, start by replacing `data/raw` extracts and extending `data/reference/region_map.csv` — the pipeline code is meant to stay stable.

---

Vusal Mammadzade · [memmedzadev81-max](https://github.com/memmedzadev81-max)
