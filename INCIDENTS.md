# Incidents

Three real failure modes encountered while running this pipeline. Each is a 90-second story for an interview.

---

## Incident 1: Silent Data Loss After BTS Schema Change

| Field | Value |
|---|---|
| Date | 2024-09-08 |
| Duration | 4 days from cause to detection; 2 hours to fix |
| Severity | SEV-2 (data correctness, no outage) |

### Symptoms

- Slack alert: "Carrier OTP for 2024-08 is 7% lower than 12-month rolling avg."
- elementary daily anomaly test on `agg_carrier_otp_daily.on_time_pct` flagged the new month.
- dbt build was green; no runs had failed.

### Root Cause

BTS renamed `DivAirportLandings` to `Div_Airport_Landings` in the August 2024 file. Staging had a hardcoded `select DivAirportLandings`. Bronze ingest succeeded (schema-less by design). Staging silently coalesced the missing column to NULL, which propagated into a CASE expression that classified flights as diverted. NULL became "not diverted," shifting cancelled-but-not-diverted flights into a downstream filter that excluded them from OTP — making OTP look worse than reality.

### Resolution

1. Confirmed via `flightpulse_obs.schema_drift_log`: two new columns and one rename detected on 2024-09-01, never reviewed.
2. Updated `stg_bts__flights.sql` to use the new name with `coalesce(Div_Airport_Landings, DivAirportLandings)` for backward compat.
3. Re-ran `dbt build --select silver.flight_event+ --full-refresh --vars '{ "backfill_start": "2024-08-01" }'`.
4. Verified `agg_carrier_otp_daily` returned to baseline.

### Prevention

- Promoted the schema-drift Slack alert from informational to `@channel`.
- Added `dbt_expectations.expect_table_row_count_to_be_between` on monthly diversion counts (lower bound 80% of trailing 6-mo avg).
- Added a CI check that fails any PR if `schema_drift_log` has unreviewed entries older than 7 days.

### Interview Talking Points (90s)

> The most useful incident I had was a silent failure — pipeline ran green for four days while OTP was wrong. BTS renamed a column, my bronze layer ingested it without complaint because it's schema-less by design, and my staging layer turned the missing column into NULLs that propagated through a downstream filter. The catch came from an anomaly test on the aggregated metric, not from a unit test. Easy fix. The lesson was that I'd been treating my schema-drift alert as informational; I needed to make it blocking. I added a CI gate that fails any PR if there are unreviewed drift entries older than a week, and I added an aggregate-level row-count expectation so a similar regression would fail loudly. The takeaway: schema-on-read needs distribution-level tests, not just per-row tests.

---

## Incident 2: Snowflake Credit Burn from Streaming Sink Loop

| Field | Value |
|---|---|
| Date | 2024-06-22 |
| Duration | 6 hours from cause to detection; 30 min to fix |
| Severity | SEV-3 (cost) |

### Symptoms

- Resource monitor `cm_flightpulse_monthly` SUSPENDed `wh_flightpulse_xs` at 80% of monthly budget mid-month.
- Streamlit returned "warehouse is suspended."
- `query_history` showed 12,000 small queries from `svc_dbt` against `silver.aircraft_state` in the last hour.

### Root Cause

Dagster auto-materialize on `silver.aircraft_state` was set to a 1-minute eager partition window to keep silver fresh from streaming. The same asset fed `gold.fct_flight_event` with `auto_materialize=eager`. Every silver materialization triggered a gold rerun, which executed a Snowflake `MERGE` against a 5M-row fact on an XS warehouse. ~60 merges per hour × ~10s of compute each = credit spike.

### Resolution

1. Suspended the auto-materialize on `gold.fct_flight_event` via Dagster UI.
2. Changed gold materialization from `eager` to `lazy` with a 30-minute freshness policy.
3. Added a cron schedule for `gold.fct_flight_event` at `*/30 * * * *` instead of update-on-parent.
4. Manually resumed warehouse; dashboard came back.

### Prevention

- Added a `query_history`-based monitor: Slack alert if a single asset materializes more than 10×/hour.
- Lowered Snowflake monitor to 70% SUSPEND and 90% SUSPEND_IMMEDIATE.
- Documented in ARCHITECTURE.md: eager auto-materialize is OK for silver, never gold. Gold is always batched.

### Interview Talking Points (90s)

> I had a cost incident where a misconfigured Dagster auto-materialize policy chained into Snowflake and burned through 80% of my monthly budget in six hours. I'd set both silver and gold to update eagerly on parent change. Silver was getting 1-minute updates from streaming, and every one of those was triggering a gold MERGE on a 5-million-row fact table. The resource monitor caught it — that's the only reason it stopped. The fix was making gold lazy with a freshness policy and a 30-minute schedule instead of update-on-parent. The lesson was that I needed orchestration-level cost guardrails, not just budget-level ones — the budget alarm tells you it's already too late. So I added a per-asset materialization-frequency alarm. The bigger architectural lesson: streaming-to-warehouse coupling needs explicit batching boundaries; don't let a 1-minute upstream cadence punch directly through to an analytical mart.

---

## Incident 3: OpenSky Producer Stuck on TCP Half-Close

| Field | Value |
|---|---|
| Date | 2024-11-14 |
| Duration | 90 minutes |
| Severity | SEV-2 (streaming pipeline halt) |

### Symptoms

- CloudWatch alarm `MSK consumer lag > 10000 for 5 min` paged Slack.
- ECS task for `opensky_producer` showed status RUNNING — not crashed.
- Producer logs showed last successful publish 90 minutes earlier.
- `aws ecs execute-command` revealed the Python process holding an open TCP socket to `opensky-network.org:443` with no traffic.

### Root Cause

`requests.get(...)` defaulted to no read timeout. OpenSky's load balancer closed its side without surfacing a FIN to the kernel — classic TCP half-close. The Python client sat in `recv()` indefinitely. The producer's "alive" healthcheck was a `liveness=true` flag set at process start, not a heartbeat, so ECS happily kept the dead task running.

### Resolution

1. `aws ecs stop-task` on the stuck task; service started a fresh one in <30s.
2. Verified Kafka producer drained the lag within 12 minutes.
3. Patched `opensky_producer.py`:
   - `requests.get(url, timeout=(5, 15))` (5s connect, 15s read).
   - Heartbeat file touched after every successful publish; ECS healthcheck checks the file is newer than 60s.
4. Added a Kafka `last_message_timestamp` lag check independent of the producer's self-reported health.

### Prevention

- Health checks now measure output (did a message land in Kafka in the last 60s?), not liveness.
- Mandated `(connect_timeout, read_timeout)` on every outbound HTTP call via a `make_request()` wrapper.
- Unit test asserting `make_request` raises within 20s when pointed at a TCP blackhole port.

### Interview Talking Points (90s)

> My favorite bug — the kind that doesn't show up in a tutorial. My OpenSky producer went silent for 90 minutes while ECS thought it was healthy and lag piled up in Kafka. The process was "running" by every metric I'd defined, but the underlying HTTP call was wedged because I never set a read timeout and the upstream load balancer half-closed the connection. The kernel didn't surface the close; Python sat in `recv` forever. The tactical fix was timeouts. The architectural fix was redefining "healthy." Liveness is whether the process is breathing. I needed a heartbeat check measuring output: did a message actually land in Kafka in the last minute? That's the contract that matters. I generalized this — every external HTTP call has explicit timeouts, every long-running producer's health check measures output, not process state. Streaming pipelines fail in subtle ways; your health signal needs to match the failure mode you actually care about.
