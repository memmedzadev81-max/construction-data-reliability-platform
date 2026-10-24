# Construction Data Reliability Platform

**Data engineering platform for road, highway and bridge programmes** — multi-source batch reliability pipeline plus near real-time delay risk scoring.

Built for the kind of data that shows up on real infrastructure work: conflicting region spellings, duplicate project IDs across contractor and PM systems, missing quality scores, late daily progress packs, and a risk score that a site engineer can actually challenge.

Related analytical work: [road-project-delay-analysis](https://github.com/memmedzadev81-max/road-project-delay-analysis) · [road-delay-risk-ml](https://github.com/memmedzadev81-max/road-delay-risk-ml)

---

## Features

| Area | What it does |
|------|----------------|
| **Data contracts** | JSON Schema validation for projects and sensor events; failed rows go to quarantine files |
| **Quality gates** | Critical and warning checks with a configurable pass threshold |
| **Batch pipeline** | Normalize → deduplicate → SCD Type 2 dimension → daily facts with late-arrival flags |
| **Risk model** | Explainable weighted score; weights live in config; feature breakdown returned with every score |
| **Streaming** | Kafka-compatible path (Redpanda) with an offline fallback on the same event seed |
| **API** | FastAPI endpoints for snapshot and event scoring |
| **Warehouse** | DuckDB by default for zero-setup local runs; Postgres DDL included for shared environments |

---

## Tech stack

- **Python 3.11+** — pandas, PyArrow, DuckDB, jsonschema
- **Contracts & quality** — JSON Schema, custom quality gates (Great Expectations available for extension)
- **Streaming** — Redpanda / Kafka (confluent-kafka), offline JSONL path
- **Serving** — FastAPI + Uvicorn
- **Infra (optional)** — Docker Compose (Postgres 16, Redpanda)
- **Tooling** — Make, pytest, ruff

---

## Quick start

```bash
git clone https://github.com/memmedzadev81-max/construction-data-reliability-platform.git
cd construction-data-reliability-platform

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

make raw       # generate realistic dirty sample data
make quality   # run quality gates
make batch     # full reliability pipeline → DuckDB + parquet
make stream    # risk scoring (works without Docker)
```

Optional services:

```bash
make up        # Postgres 16 + Redpanda
make stream    # publish + consume when the broker is up
make risk      # API at http://localhost:8080/docs
make down
```

---

## Repository layout

```
config/             settings.yaml + data contracts
data/raw            sample extracts (generated)
data/reference      region normalization map
docs/               architecture and decision log
sql/ddl             Postgres bootstrap scripts
src/batch           sample generator + reliability pipeline
src/contracts       schema validation
src/models          explainable risk scoring
src/quality         quality gates
src/streaming       producer, consumer, API, offline demo
tests/
```

---

## Design decisions

- **Quarantine by default** — a few bad budget rows must not block the daily load for the whole programme.
- **SCD Type 2 on projects** — contractor, region and plan dates change; history is kept for audit.
- **Late data is flagged, not dropped** — the site event still happened.
- **One risk function for batch and stream** — scores stay comparable across paths.
- **DuckDB first** — clone and run without waiting on containers; switch to Postgres when needed.

More detail: [docs/architecture.md](docs/architecture.md) · [docs/decisions.md](docs/decisions.md)

---

## Expected outputs

After `make raw && make batch`:

- Quarantine CSV under `data/staging/` for contract failures
- `data/processed/projects_clean.parquet` and `fact_project_daily.parquet`
- DuckDB tables: `mart.dim_project`, `mart.fact_project_daily`, `meta.pipeline_run`

After `make stream`:

- Rows in `mart.fact_risk_event` with `risk_band` and `is_late`

---

## Tests

```bash
pytest -q tests/
```

---

## Scope and limits

- Sample scale is demo-sized (~120 projects, a few hundred events).
- Risk weights are domain heuristics, not fitted model coefficients.
- Kafka security, multi-tenant isolation and full orchestrator DAGs are intentionally out of scope.

To adapt to a live programme: replace extracts under `data/raw`, extend `data/reference/region_map.csv`, and keep the pipeline entrypoints stable.

---

## License

MIT — see [LICENSE](LICENSE)

---

**Vusal Mammadzade** · [github.com/memmedzadev81-max](https://github.com/memmedzadev81-max)
