# FlightPulse

Real-time + batch flight delay intelligence pipeline. Ingests every US domestic flight from BTS (batch CSV) and live ADS-B aircraft state vectors from OpenSky (streaming), reconciles them in a medallion lakehouse (S3 → Iceberg → Snowflake), models them with dbt, orchestrates with Dagster, deploys to AWS via Terraform, and serves a Streamlit dashboard plus a FastAPI delay-prediction endpoint.

> **Project status.** This is a portfolio project. The streaming half runs **fully locally** via `docker-compose` (Redpanda for Kafka, MinIO for S3) — no cloud account required. The cloud-deployment path (MSK Serverless, Snowflake, Dagster Cloud) is fully implemented in Terraform + GitHub Actions, but **intentionally not provisioned** because steady-state cost is roughly $600–$900/month, dominated by always-on MSK Serverless. See [Cloud deployment](#cloud-deployment-optional) below.

## Architecture

```
┌────────────────────┐  ┌──────────────────────┐
│ BTS On-Time CSV    │  │ OpenSky Network REST │
│ (monthly batch)    │  │ (poll every 15s)     │
└─────────┬──────────┘  └──────────┬───────────┘
          │ Airbyte                │ Python producer
          ▼                        ▼
   ┌──────────────────────────────────────┐
   │  AWS MSK Serverless (Kafka)          │  streaming only
   └─────────────┬────────────────────────┘
                 │ consumer → Parquet
                 ▼
   ┌──────────────────────────────────────┐
   │  S3 raw bucket (BRONZE)              │  partitioned ds=YYYY-MM-DD
   └─────────────┬────────────────────────┘
                 │ dbt + Glue Catalog
                 ▼
   ┌──────────────────────────────────────┐
   │  Iceberg tables on S3 (SILVER)       │  cleaned, deduped, conformed
   └─────────────┬────────────────────────┘
                 │ Snowflake external tables → native marts
                 ▼
   ┌──────────────────────────────────────┐
   │  Snowflake (GOLD) — star schema      │  fct_flight_event + dims
   └─────────────┬────────────────────────┘
         ┌───────┴────────┐
         ▼                ▼
    Streamlit         FastAPI /predict
    dashboard         endpoint
```

Orchestrated by Dagster (local OSS or Dagster Cloud). Provisioned by Terraform. Observed by dbt elementary + CloudWatch + Slack. CI in GitHub Actions.

**Local-mode substitutions:** Redpanda for MSK Serverless, MinIO for S3, Dagster OSS for Dagster Cloud. The producer/consumer code is identical — they speak the Kafka and S3 APIs and don't know they're talking to local services.

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Batch ingestion | Airbyte OSS | Schema-drift handling out of the box; declarative source defs |
| Stream ingestion | Python + confluent-kafka | OpenSky has no Airbyte connector; lightweight producer needed |
| Streaming bus | AWS MSK Serverless | Real Kafka API, no broker mgmt, pay-per-throughput |
| Object storage | S3 | Cheap, durable, native Iceberg + Glue support |
| Table format | Apache Iceberg | ACID, schema evolution, time travel, partition evolution |
| Catalog | AWS Glue | Free, integrates with Athena and Snowflake external tables |
| Warehouse | Snowflake | Trial credit, cost transparency, resource monitors |
| Transform | dbt Core | Test-driven, lineage, contract enforcement |
| Observability | elementary-data | Open-source dbt result + freshness monitoring |
| Orchestration | Dagster Cloud (free tier) | Asset-based DAG = native lineage; better than Airflow here |
| IaC | Terraform | State-managed cloud + Snowflake provisioning |
| Serving | Streamlit + FastAPI | One-file dashboard, one-file API; Cognito-protected |
| CI/CD | GitHub Actions | Free for public repos; dbt slim CI ready |

## Prerequisites (local mode)

- macOS or Linux, Python ≥ 3.11, Docker Desktop
- (optional) free [OpenSky Network](https://opensky-network.org/) account — anonymous polling works but is rate-limited
- (optional) Snowflake 30-day trial if you want to exercise dbt against a real warehouse

That's it. No AWS account, no Snowflake account, no Dagster Cloud signup needed to run the streaming + serving stack.

## Quickstart (local, ~5 minutes)

```bash
# 1. Clone + bootstrap
git clone https://github.com/tverrier/flightpulse.git && cd flightpulse
cp .env.example .env             # OPENSKY_USER / OPENSKY_PASS optional
make venv                        # creates .venv and installs requirements

# 2. Bring up the streaming stack
#    Redpanda (Kafka-compatible) + MinIO (S3-compatible) +
#    OpenSky producer + consumer, all in docker-compose.
make stream-up

# 3. Open the operator views
make dagster-up                  # http://localhost:3000  (asset graph, sensors)
make dashboard                   # http://localhost:8501  (live aircraft map)
make api                         # http://localhost:8000/docs (FastAPI /predict)
```

**Smoke test.** Within ~30 s of `make stream-up`, the consumer logs should show 1-min Parquet files landing in MinIO at `http://localhost:9001` (login: `minioadmin` / `minioadmin`, bucket `flightpulse-raw`). The Dagster UI shows the asset graph and the four sensors (stopped by default — start them in the UI to watch the producer-health sensor tick every 60 s).

**Tear down.** `make stream-down` stops the stack; `docker volume rm flightpulse_minio_data flightpulse_heartbeat` clears local state.

### Optional: dbt against a Snowflake trial

The bronze → silver → gold dbt build needs a real Snowflake (the local MinIO substitute doesn't expose the SQL surface). If you want to run it:

```bash
# 1. Sign up for a 30-day Snowflake trial ($400 free credits)
# 2. Run the bootstrap SQL from SETUP.md §4 once
# 3. cp dbt/profiles.yml.example dbt/profiles.yml  # fill in account/user/password
make dbt-deps
make backfill MONTH=2024-01      # one BTS month → S3
make dbt-build                   # builds silver + gold against flightpulse_dev
make dbt-test                    # runs tag:critical
```

## Folder Structure

```
flightpulse/
├── README.md                     # you are here
├── ARCHITECTURE.md               # data flow + tool decisions + cost
├── SETUP.md                      # one-time setup
├── PIPELINE.md                   # ingestion + DAG + runbook
├── DATA_MODEL.md                 # tables + dbt models + lineage
├── INCIDENTS.md                  # 3 post-mortems
├── Makefile                      # all dev shortcuts
├── .env.example                  # env var template
├── infra/
│   └── terraform/                # all AWS + Snowflake provisioning
├── ingestion/
│   ├── airbyte/                  # Airbyte source/destination configs
│   ├── streaming/                # opensky_producer.py + opensky_consumer.py
│   └── seeds/                    # load_openflights.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/              # silver-layer dbt models
│   │   ├── intermediate/         # reconciliation logic
│   │   ├── marts/                # gold-layer star schema
│   │   └── sources.yml
│   ├── tests/
│   ├── macros/
│   └── snapshots/
├── orchestration/
│   └── dagster_project/          # assets, jobs, schedules, sensors
├── serving/
│   ├── dashboard/                # Streamlit app
│   └── api/                      # FastAPI app
├── tests/
│   ├── unit/
│   └── integration/
└── .github/workflows/            # ci.yml + deploy.yml
```

## Cloud deployment (optional)

The full cloud path — AWS (MSK Serverless, S3, Glue, Lambda, CloudWatch), Snowflake, Dagster Cloud, GitHub Actions OIDC — is implemented end-to-end:

- `infra/terraform/` provisions the entire AWS + Snowflake stack (~25 resources). Bootstrap sub-stack uses local state; main stack uses S3 backend.
- `.github/workflows/` runs lint + pytest + dbt-parse on every PR (`ci.yml`), `terraform plan` with auto-PR-comment (`terraform.yml`), prod dbt builds + elementary report on merges to `main` (`dbt-prod.yml`), and Dagster Cloud branch-deploys (`dagster-cloud-deploy.yml`).
- `dagster_cloud.yaml` plus `orchestration/dagster_project/Dockerfile` define a Hybrid code-location image; the Hybrid agent's IAM role and Secrets Manager entries are in `infra/terraform/dagster_cloud.tf`.
- 15 GitHub Actions secrets are catalogued in `SETUP.md` §11 with a `gh secret set` paste block.

**Why this is not currently deployed.** MSK Serverless is ~$0.75/hr ($550–700/mo) even when idle, and the project's value as a portfolio piece doesn't justify the ongoing cost. The deployment code is treated as part of the deliverable — readable, reviewable, runnable on demand — but the cluster is not running. If you fork this and want to deploy, follow `SETUP.md` end-to-end and budget accordingly.

## Documentation Index

- [ARCHITECTURE.md](./ARCHITECTURE.md) — every hop of data, tool tradeoffs, cost estimates
- [SETUP.md](./SETUP.md) — exact install + provisioning steps
- [PIPELINE.md](./PIPELINE.md) — batch + streaming specs, idempotency, runbook
- [DATA_MODEL.md](./DATA_MODEL.md) — schemas, dbt graph, lineage
- [INCIDENTS.md](./INCIDENTS.md) — real failure write-ups
