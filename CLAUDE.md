# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Source-of-truth specs

Seven markdown specs at the repo root drive every implementation decision. Treat them as canonical — if code disagrees with a spec, fix the code (or change the spec deliberately, never silently):

- `README.md` — folder layout + quickstart
- `ARCHITECTURE.md` — data flow, tool tradeoffs, partitioning, cost
- `SETUP.md` — env vars (§8), IAM policy, Snowflake bootstrap SQL
- `PIPELINE.md` — ingestion specs, idempotency strategy, schema-drift handling, Dagster DAG, failure runbook
- `DATA_MODEL.md` — gold schema column-by-column, fact/dim classification, dbt test matrix, lineage map
- `INCIDENTS.md` — three real post-mortems whose lessons are encoded into the code (see *Encoded lessons* below)
- `INTERVIEW_PREP.md` — talking points

## Common commands

All dev shortcuts go through the `Makefile`. Use `make help` to list targets.

```bash
make venv                                # create .venv, install requirements.txt
make seed-openflights                    # quarterly airport/airline reference seed
make backfill MONTH=2024-01              # re-pull one BTS month (idempotent)
make stream-up / make stream-down        # docker-compose: producer + consumer + local Kafka
make dagster-up                          # Dagster webserver + daemon @ :3000
make dashboard / make api                # Streamlit @ :8501 / FastAPI @ :8000
make snowflake-bind-integration          # finalize Snowflake storage int trust after `terraform apply`
make promote-dev-to-prod                 # row-count parity + atomic schema swap
make dbt-deps / make dbt-build / make dbt-test
make test                                # pytest tests/ (unit + integration)
make lint                                # ruff + mypy on ingestion/serving/orchestration
```

dbt commands run from the `dbt/` directory with `DBT_PROFILES_DIR=$(pwd)/dbt`. Single-model / single-test:

```bash
cd dbt
dbt build --select fct_flight_event+ --target prod
dbt test  --select source:bronze.bts_raw
dbt test  --select tag:critical                         # CI gate set
dbt run-operation log_unknown_columns \
    --args '{source_name: bronze, table_name: bts_raw, expected_columns: [...]}'
```

A single pytest:

```bash
.venv/bin/pytest tests/unit/test_make_request.py::test_blackhole_timeout -q
```

Terraform is split into a bootstrap sub-stack (S3+DynamoDB for remote state, local backend) and the main stack (S3 backend). On a fresh account run `infra/terraform/bootstrap` first, then the parent. See `infra/terraform/bootstrap/README.md`.

## Architecture in one paragraph

Medallion lakehouse with **two ingestion paths feeding one bronze bucket**: BTS monthly CSV via Airbyte (`s3://…/bts/ds=YYYY-MM-01/raw.csv`) and OpenSky REST polled every 15s by a Python producer → MSK Serverless → consumer rolling 1-min Parquet files (`s3://…/opensky/ds=…/hr=…/`). Glue catalogs the bronze prefixes; dbt staging models read them as Snowflake external tables. **Silver is dbt-owned Snowflake-managed Iceberg** on `s3://…/silver/iceberg/` via the `EV_FLIGHTPULSE_SILVER` external volume + Glue catalog integration (configured in `dbt/dbt_project.yml` under `models.flightpulse.marts.silver`). **Gold is native Snowflake** in `FLIGHTPULSE_PROD.MARTS` — a star schema (`fct_flight_event`, `dim_carrier`, `dim_airport` SCD-2, `dim_aircraft`, `dim_date`, `agg_carrier_otp_daily`). Reconciliation between BTS and OpenSky lives in `int_flight_event_reconciled` and is deliberately lossy.

## Three load-bearing facts that aren't obvious from the code

1. **BTS does not publish ICAO24.** The `aircraft` CTE in `int_flight_event_reconciled.sql` derives `icao24` from OpenSky callsigns parsed `^[A-Z]{2,3}[0-9]+$` and joined on `(carrier_code, flight_number)`. This is best-effort; the path forward is an FAA registry seed. Do **not** reintroduce a `select icao24 from stg_bts__flights` — that column does not exist.

2. **Gold is `lazy`/`incremental`/`merge`, never eager-on-parent.** This is encoded in `dbt/dbt_project.yml` (`marts.gold.+materialized: incremental`) and in the Dagster asset config. Streaming-cadence silver must not punch a 1-minute update wave through to a Snowflake `MERGE` on the fact — see Incident #2.

3. **Schema drift is schema-on-read with a CI gate.** Staging models use the `get_column_or_null` macro so renamed/removed source columns degrade to NULL rather than failing. The `log_unknown_columns` macro writes drift to `FLIGHTPULSE_OBS.SCHEMA_DRIFT.BTS_COLUMN_DRIFT`; the singular test `assert_no_unreviewed_drift_over_7_days.sql` fails CI if any drift entry is unreviewed for >7 days. The drift table is created by `ensure_drift_log_table()` in an `on-run-start` hook so the test never sees a missing table on a clean install.

## Encoded incident lessons

Each `INCIDENTS.md` post-mortem has a corresponding code defense — preserve them when refactoring:

- **#1 silent schema-drift NULLs** → `get_column_or_null` macro + `log_unknown_columns` + `assert_no_unreviewed_drift_over_7_days` singular test + `coalesce(Div_Airport_Landings, DivAirportLandings)` backward-compat in `stg_bts__flights`.
- **#2 Snowflake credit burn from streaming → MERGE loop** → gold materialization is `incremental`/lazy in `dbt_project.yml`; gold runs on a 30-min cron, never auto-materialize on parent.
- **#3 OpenSky producer wedged on TCP half-close** → `make_request()` in `ingestion/streaming/http_client.py` enforces explicit `(connect, read)` timeouts and rejects `≤0`; producer touches a heartbeat file after every successful publish; `Dockerfile.producer` HEALTHCHECK reads heartbeat mtime, not process liveness.

## Working rules for code changes

- **No stubs / no `pass` / no `NotImplementedError`** — the project is built end-to-end with runnable code only.
- **`profiles.yml` is gitignored**; `dbt/profiles.yml.example` is the template. Never commit real creds.
- **Snowflake provider is pinned `~> 0.96`** (`infra/terraform/versions.tf`) for `snowflake_external_volume`, `snowflake_catalog_integration`, and `snowflake_grant_privileges_to_account_role`. Do not regress to the deprecated `*_grant` resources.
- **Bootstrap sub-stack uses local state**; the parent stack uses S3 backend. Don't merge them.
- **Backfills always target `flightpulse_dev` first**, then `make promote-dev-to-prod`.
