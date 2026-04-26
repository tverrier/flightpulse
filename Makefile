# =============================================================================
# FlightPulse Makefile — dev shortcuts
# Targets defined per the project README.md Quickstart and SETUP.md.
# =============================================================================

SHELL          := /usr/bin/env bash
.SHELLFLAGS    := -eu -o pipefail -c
.DEFAULT_GOAL  := help

PYTHON         ?= python3.11
VENV           ?= .venv
VENV_BIN       := $(VENV)/bin
PIP            := $(VENV_BIN)/pip
PYTHON_VENV    := $(VENV_BIN)/python

DBT_PROFILES_DIR ?= $(CURDIR)/dbt
DBT_PROJECT_DIR  ?= $(CURDIR)/dbt
DAGSTER_HOME     ?= $(CURDIR)/orchestration/dagster_project/dagster_home
DAGSTER_MODULE   ?= dagster_project

# Load .env if present (export every line for child processes).
ifneq (,$(wildcard ./.env))
include .env
export
endif

# -----------------------------------------------------------------------------
.PHONY: help
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*?## "; printf "FlightPulse make targets\n\n"} \
	      /^[a-zA-Z0-9_.-]+:.*?## / {printf "  \033[36m%-32s\033[0m %s\n", $$1, $$2}' \
	      $(MAKEFILE_LIST)

# -----------------------------------------------------------------------------
# Environment
# -----------------------------------------------------------------------------
.PHONY: venv
venv: ## Create .venv and install Python requirements
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip wheel setuptools
	$(PIP) install -r requirements.txt
	@echo "venv ready — activate with: source $(VENV)/bin/activate"

.PHONY: clean
clean: ## Remove venv, dbt artifacts, caches
	rm -rf $(VENV) dbt/target dbt/dbt_packages dbt/logs .pytest_cache .ruff_cache .mypy_cache

# -----------------------------------------------------------------------------
# Seeds + ingestion
# -----------------------------------------------------------------------------
.PHONY: seed-openflights
seed-openflights: ## Quarterly seed: airports + airlines from OpenFlights → S3
	$(PYTHON_VENV) ingestion/seeds/load_openflights.py \
		--bucket $(S3_RAW_BUCKET) --prefix openflights

.PHONY: backfill
backfill: ## Re-pull one BTS month. Usage: make backfill MONTH=2024-01
	@if [ -z "$(MONTH)" ]; then echo "ERROR: MONTH=YYYY-MM is required" && exit 2; fi
	$(PYTHON_VENV) -m ingestion.airbyte.run_bts_sync \
		--month $(MONTH) \
		--bucket $(S3_RAW_BUCKET) \
		--source ingestion/airbyte/bts_source.yaml

# -----------------------------------------------------------------------------
# Streaming
# -----------------------------------------------------------------------------
.PHONY: stream-up
stream-up: ## Start producer + consumer + local Kafka in docker-compose
	docker compose -f docker-compose.yml up -d --build
	@echo "stream-up ok. Tail: docker compose logs -f opensky_producer opensky_consumer"

.PHONY: stream-down
stream-down: ## Stop streaming stack
	docker compose -f docker-compose.yml down -v

# -----------------------------------------------------------------------------
# Orchestration + serving
# -----------------------------------------------------------------------------
.PHONY: dagster-up
dagster-up: ## Launch Dagster webserver + daemon at http://localhost:3000
	mkdir -p $(DAGSTER_HOME)
	DAGSTER_HOME=$(DAGSTER_HOME) \
	  $(VENV_BIN)/dagster dev \
	  -m $(DAGSTER_MODULE) \
	  -w orchestration/dagster_project/workspace.yaml \
	  -h 0.0.0.0 -p 3000

.PHONY: dashboard
dashboard: ## Launch Streamlit dashboard at http://localhost:8501
	$(VENV_BIN)/streamlit run serving/dashboard/app.py \
	  --server.port 8501 --server.address 0.0.0.0

.PHONY: api
api: train-model ## Launch FastAPI /predict at http://localhost:8000/docs
	$(VENV_BIN)/uvicorn serving.api.main:app \
	  --host 0.0.0.0 --port 8000 --reload

.PHONY: train-model
train-model: ## Train + persist serving/api/models/arr_delay_lgbm.joblib (idempotent)
	@if [ -f serving/api/models/arr_delay_lgbm.joblib ]; then \
	  echo "model artifact present — skipping retrain (rm to force)."; \
	else \
	  $(PYTHON_VENV) -m serving.api.models.train; \
	fi

# -----------------------------------------------------------------------------
# Snowflake helpers
# -----------------------------------------------------------------------------
.PHONY: snowflake-bind-integration
snowflake-bind-integration: ## After terraform apply, finalize Snowflake storage integration trust
	$(PYTHON_VENV) infra/scripts/bind_snowflake_integration.py

.PHONY: promote-dev-to-prod
promote-dev-to-prod: ## Row-count parity check + atomic schema swap dev → prod
	$(PYTHON_VENV) infra/scripts/promote_dev_to_prod.py

# -----------------------------------------------------------------------------
# dbt + tests
# -----------------------------------------------------------------------------
.PHONY: dbt-deps dbt-build dbt-test
dbt-deps: ## dbt deps (install packages)
	cd dbt && $(VENV_BIN)/dbt deps

dbt-build: ## dbt build for prod target
	cd dbt && DBT_PROFILES_DIR=$(DBT_PROFILES_DIR) $(VENV_BIN)/dbt build --target prod

dbt-test: ## dbt tests tagged critical
	cd dbt && DBT_PROFILES_DIR=$(DBT_PROFILES_DIR) $(VENV_BIN)/dbt test --select tag:critical

.PHONY: test
test: ## Run pytest unit + integration suites
	$(VENV_BIN)/pytest -q tests/

.PHONY: lint
lint: ## ruff lint + mypy
	$(VENV_BIN)/ruff check .
	$(VENV_BIN)/mypy --ignore-missing-imports ingestion serving orchestration
