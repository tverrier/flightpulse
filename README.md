# FlightPulse

Real-time + batch flight delay intelligence pipeline. Ingests every US domestic flight from BTS (batch CSV) and live ADS-B aircraft state vectors from OpenSky (streaming), reconciles them in a medallion lakehouse (S3 → Iceberg → Snowflake), models them with dbt, orchestrates with Dagster, deploys to AWS via Terraform, and serves a Streamlit dashboard plus a FastAPI delay-prediction endpoint.

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

Orchestrated by Dagster Cloud. Provisioned by Terraform. Observed by dbt elementary + CloudWatch + Slack. CI in GitHub Actions.

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

## Prerequisites

- AWS account (free tier OK for first month)
- Snowflake trial account (30 days, $400 credit)
- OpenSky Network account (free)
- Dagster Cloud free Solo tier
- macOS or Linux dev box
- CLI tools: `git`, `aws`, `terraform`, `python ≥ 3.11`, `docker`, `dbt`, `dagster`, `snowsql`

See [SETUP.md](./SETUP.md) for exact install commands.

## Quickstart

```bash
# 1. Clone
git clone https://github.com/<you>/flightpulse.git && cd flightpulse

# 2. Configure
cp .env.example .env             # fill in AWS, Snowflake, OpenSky creds
make venv                        # creates .venv and installs requirements

# 3. Provision cloud
cd infra/terraform && terraform init && terraform apply
cd ../..

# 4. Seed reference data
make seed-openflights

# 5. Backfill 1 month of BTS
make backfill MONTH=2024-01

# 6. Start streaming producer
make stream-up

# 7. Run dbt build
dbt deps && dbt build --target prod

# 8. Launch Dagster, dashboard, API
make dagster-up   # http://localhost:3000
make dashboard    # http://localhost:8501
make api          # http://localhost:8000/docs
```

Smoke test: open the dashboard. You should see live aircraft on a map and a delay leaderboard for today.

## Folder Structure

```
flightpulse/
├── README.md                     # you are here
├── ARCHITECTURE.md               # data flow + tool decisions + cost
├── SETUP.md                      # one-time setup
├── PIPELINE.md                   # ingestion + DAG + runbook
├── DATA_MODEL.md                 # tables + dbt models + lineage
├── INCIDENTS.md                  # 3 post-mortems
├── INTERVIEW_PREP.md             # talking points + glossary
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

## Documentation Index

- [ARCHITECTURE.md](./ARCHITECTURE.md) — every hop of data, tool tradeoffs, cost estimates
- [SETUP.md](./SETUP.md) — exact install + provisioning steps
- [PIPELINE.md](./PIPELINE.md) — batch + streaming specs, idempotency, runbook
- [DATA_MODEL.md](./DATA_MODEL.md) — schemas, dbt graph, lineage
- [INCIDENTS.md](./INCIDENTS.md) — real failure write-ups
- [INTERVIEW_PREP.md](./INTERVIEW_PREP.md) — how to talk about this in a job interview
