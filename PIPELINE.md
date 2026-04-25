# Pipeline

## Ingestion

### Batch — BTS On-Time Performance

| Field | Value |
|---|---|
| Source | `https://www.transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_<YYYY>_<MM>.zip` |
| Frequency | Monthly, 5th at 06:00 UTC |
| Trigger | Dagster `bts_monthly_schedule` |
| Tool | Airbyte OSS HTTP source |
| Auth | None |
| Volume | ~600k rows / ~250 MB / month |
| Landing | `s3://flightpulse-raw/bts/ds=YYYY-MM-01/raw.csv` + `_manifest.json` |
| Idempotency | File overwritten on rerun; manifest sha256 short-circuits unchanged loads; dedupe on `(flight_date, carrier_code, flight_number, origin, scheduled_dep_ts)` |
| Retries | 3, exponential 1m / 5m / 30m |
| Failure alert | Dagster sensor → Slack |

### Stream — OpenSky State Vectors

| Field | Value |
|---|---|
| Source | `https://opensky-network.org/api/states/all` |
| Frequency | Poll every 15s |
| Tool | `ingestion/streaming/opensky_producer.py` (ECS Fargate) |
| Auth | Basic, OPENSKY_USER/PASS |
| Volume | ~10k aircraft × every 15s ≈ 60k events/min, ~1 MB/s |
| Bus | Kafka topic `opensky.states.v1`, 6 partitions, key `icao24`, retention 24h |
| Sink | `opensky_consumer.py` writes 1-min Parquet rolls to `s3://flightpulse-raw/opensky/ds=YYYY-MM-DD/hr=HH/<minute>-<uuid>.parquet` |
| Idempotency | Idempotent producer (exactly-once into Kafka); sink writes are append-only; downstream dedupes on `(icao24, event_ts)` |
| Retries | Producer auto-retries on `RD_KAFKA_RESP_ERR_*`; DLQ topic `opensky.states.dlq` for unparseable payloads |
| Failure alert | CloudWatch alarm `producer.error_rate > 1% / 5min` → SNS → Slack |

## Idempotency Strategy

| Layer | Mechanism |
|---|---|
| Bronze (BTS) | Path is `ds=YYYY-MM-01/raw.csv`; rerun overwrites identically. Manifest hash compared on every load — unchanged short-circuits downstream. |
| Bronze (OpenSky) | Append-only; per-minute file boundary means at-most-once duplicates per minute on consumer restart. |
| Silver (BTS) | dbt `incremental` model with `unique_key=['flight_date','carrier_code','flight_number','origin','scheduled_dep_ts']`, `merge` strategy. |
| Silver (OpenSky) | Iceberg `MERGE INTO` on `(icao24, event_ts)`; Iceberg snapshots provide rollback. |
| Gold | Marts are dbt incremental on a surrogate `md5(natural_key_columns)`. Backfills target a versioned schema, then atomic `ALTER SCHEMA RENAME`. |

## Schema Drift Handling

BTS adds/removes columns ~yearly. Strategy:

1. Bronze stores raw CSV verbatim; no enforcement.
2. `models/staging/stg_bts__flights.sql` selects only known columns by name with `safe_cast`. Unknown columns are ignored but logged.
3. Daily `dbt run-operation log_unknown_columns` macro queries the raw CSV header, diffs against the manifest, writes new column names to `flightpulse_obs.schema_drift_log`.
4. Slack alert `:eyes: New BTS column detected: <col>` triggers a human-review PR.
5. Removed columns: dbt staging coalesces missing → NULL via `{{ get_column_or_null('col_name') }}` macro.
6. CI gate fails any PR if `schema_drift_log` has unreviewed entries older than 7 days.

OpenSky drift is rare (versioned API); same macro pattern applied.

## DAG (Dagster Assets)

```
opensky_states_raw (sensor, every 1 min) ─┐
                                          ├─► silver.aircraft_state (dbt)
openflights_airports_raw (quarterly) ─────┤
openflights_airlines_raw (quarterly) ─────┤
                                          ├─► silver.dim_airport_seed
                                          └─► silver.dim_airline_seed
bts_monthly_raw (monthly cron) ─► silver.flight_event ─┐
                                                       ├─► gold.fct_flight_event
silver.aircraft_state ─────────────────────────────────┤
silver.dim_airport_seed ─► gold.dim_airport ───────────┤
silver.dim_airline_seed ─► gold.dim_carrier ───────────┤
                          gold.dim_aircraft ───────────┤
                          gold.dim_date (static seed) ─┘
                                  │
                                  ▼
                       gold.agg_carrier_otp_daily
                                  │
                                  ▼
                    elementary tests + freshness
```

| Asset | Depends on | Schedule / sensor | Retries | SLA | Alert |
|---|---|---|---|---|---|
| `bts_monthly_raw` | none | Cron `0 6 5 * *` | 3, exp 1/5/30m | files in S3 ≤1h | Slack |
| `opensky_states_raw` | none | Sensor every 60s checks ECS task health | n/a (long-running) | producer lag <30s | CloudWatch |
| `openflights_*` | none | Cron `0 0 1 */3 *` | 2 | within 6h | Slack |
| `silver.flight_event` | `bts_monthly_raw` | auto-materialize on parent update | 2 | freshness ≤24h | Dagster freshness policy |
| `silver.aircraft_state` | `opensky_states_raw` | every 5 min, partition window | 2 | freshness ≤30min | Slack |
| `gold.fct_flight_event` | silver.flight_event, silver.aircraft_state | **lazy**; cron `*/30 * * * *` | 2 | ≤1h | Slack |
| `gold.dim_*` | seeds | auto on parent | 2 | ≤6h | Slack |
| `agg_carrier_otp_daily` | `gold.fct_flight_event` | daily 03:00 UTC | 2 | ≤2h | Slack |
| `elementary_run` | all gold | daily 04:00 UTC | 1 | n/a | Slack on test fail |

> **Note:** Gold materialization is `lazy` not `eager`. See [INCIDENTS.md § Incident 2](./INCIDENTS.md#incident-2-snowflake-credit-burn-from-streaming-sink-loop).

## Backfill Procedure

```bash
# 1. Pick range
START=2023-01-01 END=2023-12-01

# 2. Re-pull bronze for each month (idempotent)
for m in $(seq -f "%02g" 1 12); do
  make backfill MONTH=2023-$m
done

# 3. Force silver rebuild for the partition window
dbt build --select silver.flight_event+ \
  --vars '{ "backfill_start": "2023-01-01", "backfill_end": "2023-12-31" }' \
  --full-refresh

# 4. Refresh dependent gold marts
dbt build --select gold+ \
  --vars '{ "backfill_start": "2023-01-01", "backfill_end": "2023-12-31" }'

# 5. Validate
dbt test --select tag:critical state:modified
```

Backfill always runs against `flightpulse_dev` first; promote with `make promote-dev-to-prod` after row-count parity check.

## Failure Runbook

| Failure mode | Detection | Diagnosis | Resolution |
|---|---|---|---|
| BTS download 404 | `bts_monthly_raw` Dagster fail | BTS sometimes posts late; check URL pattern | Retry next day; if persistent, switch to BTS mirror |
| Airbyte schema-drift error | dbt staging fails "column does not exist" | `select * from flightpulse_obs.schema_drift_log order by detected_at desc limit 5` | Update `stg_bts__flights.sql` or `get_column_or_null` macro; rerun |
| OpenSky 429 rate limit | Producer error rate alarm | Anonymous tier hit | Confirm OPENSKY_USER set; if yes, raise interval to 30s |
| MSK consumer lag | CloudWatch `kafka.consumer.lag.max > 10000` | Sink container OOM or S3 throttled | Scale ECS task to 2; raise consumer batch size; multi-prefix S3 writes |
| dbt test fail (severity=error) | Slack alert from elementary | Open elementary UI → row sample | Quarantine: rerun upstream; if data is bad, mark `test:warn` for that snapshot, file ticket |
| Snowflake credit overrun | Resource monitor SUSPEND alert | `SHOW WAREHOUSES;` confirms suspended | Investigate `query_history` ordered by credits used; tune or kill; raise quota only with approval |
| Streaming producer "running but silent" | `last_message_timestamp` lag check | TCP half-close, see Incident #3 | Restart task; verify timeouts and heartbeat in `make_request()` wrapper |
| Dashboard returns no data | User report | Check `gold.fct_flight_event` `max(loaded_at)` and Dagster materialization timeline | Find broken asset, retry; backfill failed window if upstream |
