# Decision log

Short notes so future-me (or a reviewer) knows why something looks “odd”.

## 2026-10 — DuckDB first, Postgres second

Portfolio clones die when the first step is “install Postgres + wait for healthchecks”. DuckDB keeps `make batch` working offline. DDL for Postgres stays in-repo so moving to a shared warehouse is a connection string change, not a rewrite.

## 2026-10 — Quarantine over hard fail by default

`quarantine_on_fail: true`. On a real programme of works, blocking the entire daily load because 3 of 120 rows have a bad budget is how you lose trust with the PMO. Critical checks still surface loudly; bad rows are isolated.

## 2026-10 — Explainable risk, not a trained model

I already have a separate ML experiment on delay classification. This platform’s score has to be something a site engineer can disagree with using the feature breakdown. Weights live in `config/settings.yaml`.

## 2026-10 — Offline streaming fallback

Demoing Kafka in every interview environment is fragile. `demo_run` scores the same JSONL seed when the broker is down so the risk path is still visible.

## 2026-10 — Region map as data, not code

`Bakı` / `Baku` / `BAKU` will keep growing. Putting the map in `data/reference/region_map.csv` means ops can extend it without a deploy of scoring logic.
