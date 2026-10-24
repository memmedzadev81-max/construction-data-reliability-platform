.PHONY: help setup up down raw quality batch stream risk test lint clean

help:
	@echo "Construction Data Reliability Platform"
	@echo ""
	@echo "  make setup     - create venv and install deps"
	@echo "  make up        - start Postgres + Redpanda (docker)"
	@echo "  make down      - stop containers"
	@echo "  make raw       - generate / refresh dirty sample data"
	@echo "  make quality   - run data quality gates on raw"
	@echo "  make batch     - run full batch reliability pipeline"
	@echo "  make stream    - produce + consume sensor events (demo)"
	@echo "  make risk      - start local risk scoring API"
	@echo "  make test      - unit tests"
	@echo "  make clean     - remove generated artifacts"

setup:
	python -m venv .venv
	.venv/bin/pip install -U pip
	.venv/bin/pip install -r requirements.txt
	@echo "Activate with: source .venv/bin/activate"

up:
	docker compose up -d
	@echo "Waiting for services..."
	@sleep 8
	@echo "Postgres: localhost:5432  |  Redpanda: localhost:9092"

down:
	docker compose down -v

raw:
	python -m src.batch.generate_sample_data

quality:
	python -m src.quality.run_checks

batch:
	python -m src.batch.pipeline

stream:
	python -m src.streaming.demo_run

risk:
	uvicorn src.streaming.risk_api:app --reload --port 8080

test:
	pytest -q tests/

lint:
	ruff check src tests

clean:
	rm -rf data/processed data/staging data/warehouse logs .tmp *.duckdb
	find . -type d -name __pycache__ -exec rm -rf {} +
