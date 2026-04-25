# Architecture

## Data Flow

1. **Source → Bronze (raw)**
   - **BTS batch:** Airbyte pulls monthly ZIP from `transtats.bts.gov`, unzips, lands raw CSV at `s3://flightpulse-raw/bts/ds=YYYY-MM-DD/raw.csv` plus a sidecar `_manifest.json` containing file sha256 and `ingested_at`. Triggered by Dagster monthly schedule (5th of month, 06:00 UTC).
   - **OpenSky stream:** `opensky_producer.py` polls REST endpoint every 15s, publishes JSON to Kafka topic `opensky.states.v1` keyed by `icao24`. `opensky_consumer.py` rolls 1-minute Parquet files to `s3://flightpulse-raw/opensky/ds=YYYY-MM-DD/hr=HH/<minute>-<uuid>.parquet`.
   - **OpenFlights:** `load_openflights.py` Dagster asset, runs quarterly, writes `airports.csv` and `airlines.csv` under `s3://flightpulse-raw/openflights/`.

2. **Bronze → Silver (Iceberg)**
   - dbt models in `models/staging/` read from Glue-cataloged external tables over the bronze prefixes.
   - Cleansed, typed, deduped on natural keys, stored as Iceberg tables under `s3://flightpulse-silver/`.
   - Hidden-partitioned (Iceberg) on `month(flight_date)` for the fact, `day(event_ts)` for state vectors.

3. **Silver → Gold (Snowflake)**
   - Snowflake reads silver via Iceberg external tables.
   - dbt `marts/` materializes the star schema natively in Snowflake schema `FLIGHTPULSE_PROD.MARTS`.
   - Incremental on the fact; full-refresh on dims.

4. **Gold → Consumers**
   - Streamlit reads Snowflake via `snowflake-connector-python` with role `RL_DASHBOARD_RO`.
   - FastAPI `/predict` reads silver Iceberg via `pyiceberg` and a cached LightGBM model.
   - Both fronted by CloudFront with Cognito auth.

5. **Observability sidecar**
   - elementary writes test results + freshness to `FLIGHTPULSE_OBS.elementary.*`.
   - Dagster sensor watches elementary alerts and posts to Slack `#flightpulse-alerts`.

## Tool Decisions

| Decision | Chose | Considered | Why |
|---|---|---|---|
| Orchestrator | Dagster | Airflow, Prefect | Asset model = free lineage; SDA maps 1:1 to dbt; better local dev |
| Table format | Iceberg | Delta, Hudi | Native Snowflake read; cleanest partition evolution; vendor-neutral |
| Streaming bus | MSK Serverless | Kinesis, Redpanda Cloud | Real Kafka skill; no broker mgmt; cheaper than provisioned MSK |
| Warehouse | Snowflake | BigQuery, Redshift Serverless | Trial covers project; resource monitors; cost transparency |
| Stream ingest | Custom Python | Kafka Connect REST source | OpenSky paginates oddly; custom is simpler than bending a connector |
| Batch ingestion | Airbyte OSS | Fivetran, raw Python | Open-source; declarative; built-in schema-drift handling |
| Transform | dbt Core | SQLMesh, Spark SQL | Industry standard; deepest test ecosystem; native Snowflake |
| IaC | Terraform | CDK, Pulumi | Mature Snowflake provider; HCL is more readable in PRs |

## Layer Definitions

| Layer | Storage | Contents | SLA |
|---|---|---|---|
| **Bronze (raw)** | `s3://flightpulse-raw/` | Source files as-received, immutable, partitioned by ingest date. No schema enforcement. | Lossless retention; 3-yr Glacier transition |
| **Silver (staged)** | Iceberg on `s3://flightpulse-silver/` + Glue Catalog | Typed, deduped, conformed; one row per natural key per event time. | Freshness ≤ 30 min stream, ≤ 1 day batch; dbt tests pass |
| **Gold (serving)** | Snowflake `FLIGHTPULSE_PROD.MARTS` | Star schema: `fct_flight_event`, `dim_carrier`, `dim_airport`, `dim_aircraft`, `dim_date`, `agg_carrier_otp_daily` | Freshness ≤ 1 hr; row-count checks on all marts |

## Partitioning & Clustering

| Table | Partition by | Cluster / sort by | Reasoning |
|---|---|---|---|
| `bronze.bts_raw` (S3 path) | `ds=YYYY-MM-DD` | n/a | Date of ingest matches Airbyte sync cadence |
| `bronze.opensky_raw` (S3 path) | `ds=YYYY-MM-DD/hr=HH` | n/a | Hourly hot files; cheap purge by date |
| `silver.flight_event` (Iceberg) | `month(flight_date)` (hidden) | `(carrier_code, flight_date)` | 90% of queries are by carrier+month; hidden partitions allow evolution |
| `silver.aircraft_state` (Iceberg) | `day(event_ts)` (hidden) | `(icao24, event_ts)` | Track-by-aircraft queries dominate |
| `gold.fct_flight_event` (Snowflake) | n/a | cluster `(flight_date, carrier_code)` | Auto-clustering aligned to dashboard filters |
| `gold.dim_*` | n/a | none | <1M rows; full scans are free |

## Cost Estimates

Assumed regions: AWS `us-east-1`, Snowflake `AWS_US_EAST_1`.

| Component | Dev/local | Small prod (1 mo BTS, 1 region OpenSky) | Full prod (5 yr BTS, global OpenSky) |
|---|---|---|---|
| S3 storage | <1 GB / $0.05 | 50 GB / $1.15 | 4 TB / $92 |
| S3 requests | negligible | $5 | $40 |
| MSK Serverless | off | $30 | $220 |
| Glue Catalog | free | free | $5 |
| Athena | $0 | $5 | $40 |
| Snowflake compute | $0 (suspended) | $40 (XS, 2 hr/day) | $400 (S, auto-suspend) |
| Snowflake storage | free | $5 | $80 |
| Dagster Cloud | free | free | $300 (Pro) |
| Cognito + CloudFront | $0 | $1 | $15 |
| **Total / month** | **~$0** | **~$87** | **~$1,200** |

Resource monitors:
- Snowflake `cm_flightpulse_monthly` suspends warehouse at 80% of $50 budget (dev) / $500 (full prod).
- AWS Cost Anomaly Detection + Budget alarm at $25 (dev), $150 (small), $1,500 (full).
