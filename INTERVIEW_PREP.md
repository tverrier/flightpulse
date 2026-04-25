# Interview Prep — FlightPulse

## 15 Likely Technical Questions

### 1. Walk me through your architecture.

Frame: Source → Bronze (S3) → Silver (Iceberg + Glue) → Gold (Snowflake) → Consumer (Streamlit + FastAPI). Orchestrated by Dagster, provisioned by Terraform. Mention dual ingest: BTS batch monthly via Airbyte, OpenSky streaming via Kafka. Land on the *reconciliation* problem — same flights, two sources, different latency.

### 2. Why streaming AND batch? Why not pick one?

Different SLAs. BTS is the system of record but lands 30 days late; OpenSky is real-time but unauthoritative (transponder gaps, tail-num mapping). The pipeline reconciles them — `reconciliation_status` records which source was used and whether they matched. Realistic — most enterprise data lands in both modes.

### 3. How do you handle idempotency?

Layer-by-layer: bronze paths are deterministic (`ds=YYYY-MM-DD/raw.csv`), so a rerun overwrites in place. Silver uses Iceberg `MERGE INTO` on natural keys. Gold dbt incrementals key on a md5 surrogate. Manifest hash short-circuits unchanged loads downstream.

### 4. How do you handle schema drift in BTS?

Bronze stores raw CSV — no enforcement. Staging selects only known columns by name with a `get_column_or_null` macro. A daily macro diffs the CSV header against the manifest and writes new columns to `flightpulse_obs.schema_drift_log`. Slack alert for review. CI gate fails any PR if drift entries are older than 7 days. (Then tell incident #1 if asked for an example.)

### 5. Why Dagster over Airflow?

Dagster's asset model gives you free lineage that maps 1:1 onto dbt models — the whole graph in one UI. Airflow's DAG model treats a dbt run as one opaque task; you lose model-level lineage in the orchestrator. Local dev is faster (hot-reload). Tradeoff: Airflow has a bigger ecosystem and more enterprise adoption.

### 6. Why Iceberg specifically?

Three reasons: schema evolution (rename without rewriting data), partition evolution (change strategy without rewriting), time travel (snapshots make rollback trivial). And Snowflake reads Iceberg natively — silver lives in cheap S3, no warehouse storage cost for raw history.

### 7. How do you partition the fact table?

Silver Iceberg uses hidden partitioning on `month(flight_date)` — hidden so I can change strategy without breaking queries. Gold in Snowflake auto-clusters on `(flight_date, carrier_code)`. 90% of dashboard queries filter on date+carrier; clustering aligns the warehouse with that. I avoided clustering on origin_airport because cardinality (~400) is low enough that micro-partition pruning happens for free.

### 8. How do you test data quality?

Three layers. Schema tests (`unique`, `not_null`, `relationships`) on all keys. Distribution tests (`dbt_expectations.expect_column_quantile_values_to_be_between`) on numeric metrics — these catch silent regressions. Custom singular tests (e.g. `assert_no_future_actual_arrivals.sql`). All `error`-severity tests are tagged `critical` and run on every PR via dbt slim CI. elementary publishes results to Slack and a metrics dashboard.

### 9. How do you handle late-arriving data?

BTS publishes corrections up to 30 days after the original month. Silver merge logic uses `MERGE INTO` keyed on `(flight_date, carrier, flight_number, origin, scheduled_dep_ts)` so updates land in place. Gold incremental window is 35 days rolling, so corrections within that window propagate automatically. Older corrections go through the manual backfill procedure in PIPELINE.md.

### 10. What happens if Kafka is down?

Producer buffers in memory up to 10 MB (~10 min of OpenSky), then drops with a metric. OpenSky doesn't replay, so anything older is lost — acceptable because BTS is the system of record. Alarm fires at >5% drop rate. The streaming pipeline never replays; BTS fills the canonical record.

### 11. How would you scale this 10×?

Three bottlenecks first. (1) Kafka partitions — currently 6, scale to 24+. (2) Snowflake — keep XS for batch, switch to S with auto-suspend for the gold incremental, lean harder on clustering. (3) Iceberg compaction — add a daily Glue `OPTIMIZE`+`VACUUM` job to keep small-file count down. Cost-wise, 10× lands at ~$1,200/month per ARCHITECTURE.md.

### 12. How is this deployed?

Terraform for everything in AWS plus Snowflake (Snowflake provider). Remote state in S3 with a DynamoDB lock table. GitHub Actions runs `terraform plan` on every PR, `terraform apply` on merge to main, and `dbt build` against a CI Snowflake schema. Dagster Cloud agent runs on ECS Fargate. Rollback is `git revert` + workflow.

### 13. How do you monitor this?

Three signals. (1) Correctness — dbt + elementary, Slack alerts on test fails. (2) Freshness — Dagster freshness policies on every asset, page if stale. (3) Cost — Snowflake resource monitors and AWS Cost Anomaly Detection, plus a per-asset materialization-frequency alarm I added after incident #2. CloudWatch covers the streaming containers.

### 14. What's the consumer-facing layer?

Streamlit dashboard, Snowflake-backed: `agg_carrier_otp_daily` for trends, `fct_flight_event` for filtered drill-downs. Cognito auth, CloudFront. FastAPI `/predict` takes `(carrier, origin, dest, scheduled_dep_ts)` and returns delay probability from a LightGBM model trained on the silver fact. Both run on ECS Fargate.

### 15. What was the hardest part?

Reconciliation between BTS and OpenSky. BTS uses tail numbers, OpenSky uses ICAO24 hex codes — the join goes through `dim_aircraft` and isn't always clean (registrations transfer, tail numbers get reused). `int_flight_event_reconciled` is the most-tested model in the project; the `reconciliation_status` column comes from there. It also forced me to make the live arrival timestamp explicitly *provisional* — BTS actual is preferred when available, OpenSky is a placeholder.

## 5 Questions to Ask the Interviewer

1. *"How is your team currently handling late-arriving and corrected data — at the source, in transformation, or via warehouse merge logic?"* — reveals their maturity around real-world data.
2. *"Do you have an established medallion or lakehouse pattern, or is each team picking their own storage layer?"* — tells you whether you'd own architecture or inherit one.
3. *"What does your data quality and observability stack look like? dbt tests, elementary, Monte Carlo, in-house?"* — signals whether QA is a discipline or an afterthought.
4. *"How is cost ownership split — does the data team own warehouse spend, or is it allocated back to consuming teams?"* — surfaces the org's political shape.
5. *"What's the most common cause of pipeline incidents on your team right now?"* — tells you what you'd actually be fixing in your first 90 days.

## 5-Minute Walkthrough Arc

| Time | Show | Emphasize |
|---|---|---|
| 0:00–0:30 | README architecture diagram | "Two ingestion modes feeding a medallion lakehouse, ending in a star schema with two consumers." |
| 0:30–1:30 | Streamlit dashboard, live | Real flights on the map. "Reading Snowflake gold; dots are coming from OpenSky streaming with sub-30-second latency." |
| 1:30–2:30 | Dagster UI, asset graph | Lineage. "Every box is a dbt model or ingestion task; Dagster gives me freshness SLAs and partition-aware backfill." |
| 2:30–3:30 | INCIDENTS.md, Incident #1 | "Real silent failure I shipped, how I caught it, what I added so a similar regression fails loud." |
| 3:30–4:30 | dbt docs site | Click through `fct_flight_event` lineage from raw column to dashboard. Show the tests. |
| 4:30–5:00 | ARCHITECTURE.md cost section | "Runs ~$87/month at small-prod scale, ~$1,200 at full prod. Here are the resource monitors that enforce it." |

What to skip: every Terraform file (mention it exists), every dbt model body (show the graph instead), the FastAPI source (mention the endpoint, not the code).

## Behavioral STAR Prompts

| Question | What this project lets you say |
|---|---|
| "Tell me about a time you caught a bug in production." | Incident #1 — silent schema drift, anomaly test caught it, prevention shipped. |
| "Describe a costly mistake and how you handled it." | Incident #2 — Dagster auto-materialize burned 80% of monthly budget; redesigned eager-vs-lazy materialization rules. |
| "Tell me about debugging something hard to reproduce." | Incident #3 — TCP half-close; redefined "healthy" from liveness to output. |
| "Tell me about balancing competing priorities." | Streaming freshness vs. warehouse cost — chose batched gold updates with a 30-min freshness SLA over real-time gold. |
| "Tell me about a technical decision you'd defend." | Iceberg over Delta — vendor-neutrality, partition evolution, native Snowflake read. |
| "Tell me about a time you improved a system you inherited." | Schema-drift handling: started as informational alerts, evolved into a CI gate after the silent regression. |

## Glossary (plain English)

| Term | Definition |
|---|---|
| Medallion architecture | Three-tier storage: bronze (raw, untouched), silver (cleaned & conformed), gold (modeled for consumption). |
| Idempotent | A pipeline that produces the same output if you run it twice. Re-runs do not create duplicates. |
| Schema drift | When the source data's columns change over time (added, removed, renamed) without warning. |
| Star schema | One fact table in the middle, dimension tables around it; matches how BI tools query data. |
| Fact table | A table of events or measurements (one row = one thing that happened). |
| Dimension table | Descriptive context about the things in the fact table (carrier names, airport locations). |
| SCD-2 | Slowly Changing Dimension type 2 — keep historical versions of a dim row by adding `valid_from` / `valid_to`. |
| SCD-1 | Slowly Changing Dimension type 1 — overwrite changes; no history kept. |
| Surrogate key | A meaningless ID column (e.g. md5 hash) used to uniquely identify a row independent of business keys. |
| Natural key | The real-world business identifier (e.g. flight date + carrier + flight number). |
| Partitioning | Splitting a table by a column (date, region) so queries only scan the relevant slice. |
| Clustering | Sorting data within partitions by another column to make filtered queries faster. |
| Iceberg | Open table format on cloud object storage that adds ACID, schema evolution, and time travel to S3 files. |
| dbt | SQL-based transformation tool that turns SELECT statements into models with tests, docs, and lineage. |
| Orchestrator | Software that schedules and monitors data pipelines, handles retries, exposes lineage (Dagster, Airflow). |
| DAG | Directed acyclic graph — the dependency tree of pipeline tasks. |
| Backfill | Reprocessing a historical date range, usually because a bug was fixed or new logic was added. |
| Late-arriving data | Records that show up after the period they belong to has been "closed." |
| Reconciliation | Comparing two sources of the same truth and resolving disagreements with rules. |
| MERGE INTO | SQL statement that inserts new rows and updates existing ones based on a key — the idempotent workhorse. |
| dbt test | A SQL query that returns 0 rows on success; if it returns any rows, the test fails. |
| Freshness SLA | The maximum acceptable age of data in a table before someone gets paged. |
| Resource monitor | Snowflake feature that suspends a warehouse when credit usage hits a threshold. |
| Slim CI | Running only the dbt models that changed in a PR plus their downstreams, instead of the whole graph. |
| Kafka topic | An ordered, partitioned log of messages — producers write, consumers read independently. |
| ADS-B | Automatic Dependent Surveillance-Broadcast — the protocol aircraft use to broadcast position; basis of OpenSky data. |
| ICAO24 | A unique 24-bit hex code permanently assigned to an aircraft transponder. |
| IaC | Infrastructure as Code — provisioning cloud resources via versioned config files (Terraform) instead of clicking. |
| Auto-materialize | Dagster feature where a downstream asset rebuilds automatically when its upstream changes. |
| TCP half-close | A network failure mode where one side closes the connection without the other side noticing. |
