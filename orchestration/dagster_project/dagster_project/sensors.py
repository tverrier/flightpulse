"""Sensors — the event-driven half of the pipeline.

Three sensors, all directly traceable to PIPELINE.md / INCIDENTS.md:

  * `opensky_freshness_sensor`
        — every 60 s, `observable_source_asset` opensky_states_raw is
        re-evaluated. If the latest S3 object's mtime hasn't advanced in
        >120 s we infer the producer has wedged (Incident #3) and emit a
        Slack alert.

  * `silver_aircraft_state_sensor`
        — when opensky_states_raw advances its DataVersion, kick a partial
        run of silver_aircraft_state so silver stays ≤30 min fresh per the
        SLA. Gold is *not* triggered here (lazy materialization rule).

  * `dagster_failure_sensor`
        — any run failure across the code location → Slack post.
"""

from __future__ import annotations

import datetime as dt
import os
from typing import Any

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    define_asset_job,
    run_failure_sensor,
    sensor,
)
from dagster_aws.s3 import S3Resource

from dagster_project.constants import (
    S3_OPENSKY_PREFIX,
    S3_RAW_BUCKET,
)
from dagster_project.resources import SlackAlertResource, SnowflakeResource

# -----------------------------------------------------------------------------
# Job triggered by the opensky-freshness sensor — narrow selector so we only
# rebuild silver_aircraft_state, never gold (lazy rule).
# -----------------------------------------------------------------------------
silver_aircraft_state_refresh_job = define_asset_job(
    name="silver_aircraft_state_refresh_job",
    selection=AssetSelection.keys(AssetKey(["silver_aircraft_state"])),
    description="Triggered by opensky_states_raw freshness sensor.",
    tags={"warehouse": "snowflake", "layer": "silver", "trigger": "freshness"},
)


# -----------------------------------------------------------------------------
# OpenSky producer-health sensor.
# -----------------------------------------------------------------------------
_PRODUCER_LAG_WARN_SECONDS = 120  # tighter than the 30s SLA — alert before silver SLA breach
_PRODUCER_LAG_FATAL_SECONDS = 600


@sensor(
    name="opensky_producer_health_sensor",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Watches the latest object under s3://${S3_RAW_BUCKET}/opensky/. "
        "If lag exceeds 2 min, posts a Slack warning; >10 min posts FATAL. "
        "Encodes Incident #3 (TCP half-close) detection."
    ),
)
def opensky_producer_health_sensor(
    context: SensorEvaluationContext,
    s3: S3Resource,
    slack: SlackAlertResource,
) -> SensorResult | SkipReason:
    bucket = os.environ.get("S3_RAW_BUCKET", S3_RAW_BUCKET)
    today = dt.datetime.now(dt.timezone.utc).date()
    yesterday = today - dt.timedelta(days=1)

    client = s3.get_client()
    latest: dt.datetime | None = None
    for ds in (today.isoformat(), yesterday.isoformat()):
        prefix = f"{S3_OPENSKY_PREFIX}/ds={ds}/"
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}
        ):
            for obj in page.get("Contents", []) or []:
                lm = obj.get("LastModified")
                if isinstance(lm, dt.datetime) and (latest is None or lm > latest):
                    latest = lm

    now = dt.datetime.now(dt.timezone.utc)
    if latest is None:
        slack.post(
            f":rotating_light: opensky_states_raw — no objects in s3://{bucket}/{S3_OPENSKY_PREFIX}/ "
            f"(today or yesterday). Producer may be stopped."
        )
        return SkipReason("no opensky objects yet")

    lag = (now - latest).total_seconds()
    context.update_cursor(latest.isoformat())
    if lag > _PRODUCER_LAG_FATAL_SECONDS:
        slack.post(
            f":rotating_light: *FATAL* opensky producer lag = {int(lag)}s "
            f"(latest object: {latest.isoformat()}). See INCIDENTS.md #3."
        )
    elif lag > _PRODUCER_LAG_WARN_SECONDS:
        slack.post(
            f":warning: opensky producer lag = {int(lag)}s "
            f"(latest object: {latest.isoformat()})."
        )

    # No run requests — this sensor is purely observational.
    return SensorResult(run_requests=[])


# -----------------------------------------------------------------------------
# Asset-driven sensor: when the opensky_states_raw observable source advances
# its DataVersion, materialize silver_aircraft_state.
#
# We implement this with a "every-N-seconds" sensor that records the cursor
# rather than the more elaborate AutoMaterializeSensor — that keeps the wiring
# explicit and easy to test, and matches the PIPELINE.md wording ("every
# 5 min, partition window").
# -----------------------------------------------------------------------------
@sensor(
    name="silver_aircraft_state_freshness_sensor",
    minimum_interval_seconds=60 * 5,
    job=silver_aircraft_state_refresh_job,
    default_status=DefaultSensorStatus.STOPPED,
    description="Kick silver_aircraft_state every 5 min if new opensky data has landed.",
)
def silver_aircraft_state_freshness_sensor(
    context: SensorEvaluationContext,
    s3: S3Resource,
) -> SensorResult | SkipReason:
    bucket = os.environ.get("S3_RAW_BUCKET", S3_RAW_BUCKET)
    today = dt.datetime.now(dt.timezone.utc).date()
    prefix = f"{S3_OPENSKY_PREFIX}/ds={today.isoformat()}/"

    client = s3.get_client()
    latest: dt.datetime | None = None
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}
    ):
        for obj in page.get("Contents", []) or []:
            lm = obj.get("LastModified")
            if isinstance(lm, dt.datetime) and (latest is None or lm > latest):
                latest = lm

    if latest is None:
        return SkipReason("no opensky objects today")

    cursor = latest.isoformat()
    if context.cursor == cursor:
        return SkipReason(f"no new objects since {cursor}")

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"silver-aircraft-state-{cursor}",
                tags={"trigger": "opensky_freshness", "latest_object_mtime": cursor},
            )
        ],
        cursor=cursor,
    )


# -----------------------------------------------------------------------------
# Failure → Slack. Catches every run in the code location.
# -----------------------------------------------------------------------------
@run_failure_sensor(
    name="dagster_failure_to_slack_sensor",
    default_status=DefaultSensorStatus.STOPPED,
    description="Posts a Slack message on any run failure across the code location.",
)
def dagster_failure_to_slack_sensor(
    context: RunFailureSensorContext,
    slack: SlackAlertResource,
) -> None:
    run = context.dagster_run
    job_name = run.job_name
    run_id = run.run_id
    failure_msg = context.failure_event.message or "no message"
    slack.post(
        f":x: Dagster run failed — *{job_name}* "
        f"(run_id={run_id[:8]}…)\n```{failure_msg[:1200]}```"
    )
    context.log.info("posted failure alert to slack for run %s", run_id)


# -----------------------------------------------------------------------------
# Elementary alerts → Slack.
#
# elementary's on-run-end hooks (configured in dbt_project.yml) write rows to
# FLIGHTPULSE_OBS.elementary.alerts every time dbt runs. This sensor polls
# that table every 5 min, forwards new rows to Slack via SlackAlertResource,
# and advances its cursor to the max(detected_at) it has already posted so a
# restart never double-posts.
#
# Severity routing:
#   * detected_at within last 24 h + alert_class in ('test','model_error') →
#     :rotating_light: (paged via #flightpulse-alerts).
#   * everything else                                                       →
#     :warning: (informational, same channel).
# -----------------------------------------------------------------------------
_ELEMENTARY_ALERTS_QUERY = """
select
    alert_id,
    detected_at,
    alert_class,
    model_unique_id,
    test_name,
    status,
    alert_description,
    tags
from FLIGHTPULSE_OBS.elementary.alerts
where detected_at > %s
order by detected_at asc
limit 100
""".strip()


@sensor(
    name="elementary_alerts_sensor",
    minimum_interval_seconds=60 * 5,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Forwards new rows in FLIGHTPULSE_OBS.elementary.alerts to "
        "#flightpulse-alerts. Cursor = max(detected_at) already posted."
    ),
)
def elementary_alerts_sensor(
    context: SensorEvaluationContext,
    snowflake: SnowflakeResource,
    slack: SlackAlertResource,
) -> SkipReason | SensorResult:
    # Cursor format: ISO-8601 UTC. Default to "1 hour ago" so first run is
    # bounded — we don't want a fresh sensor to flood Slack with year-old rows.
    if context.cursor:
        since = context.cursor
    else:
        since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat()

    try:
        # query_one returns a single row; we need many → use _connect directly.
        with snowflake._connect() as conn, conn.cursor() as cur:
            cur.execute(_ELEMENTARY_ALERTS_QUERY, (since,))
            rows = cur.fetchall() or []
    except Exception as exc:  # pragma: no cover — defensive: missing schema on cold install
        context.log.warning("elementary_alerts_sensor query failed: %s", exc)
        return SkipReason(f"elementary alerts query failed: {exc}")

    if not rows:
        return SkipReason(f"no new elementary alerts since {since}")

    posted = 0
    max_detected: dt.datetime | None = None
    for row in rows:
        alert_id, detected_at, alert_class, model_uid, test_name, status, desc, tags = row
        if isinstance(detected_at, dt.datetime) and (max_detected is None or detected_at > max_detected):
            max_detected = detected_at

        is_critical = (alert_class in ("test", "model_error")) and (
            isinstance(tags, str) and "critical" in tags.lower()
        )
        icon = ":rotating_light:" if is_critical else ":warning:"
        target = test_name or model_uid or "<unknown>"
        slack.post(
            f"{icon} *elementary* {alert_class}/{status} — `{target}`\n"
            f"{(desc or '')[:1500]}\n"
            f"_alert_id={alert_id} detected_at={detected_at}_"
        )
        posted += 1

    if max_detected is not None:
        context.update_cursor(max_detected.isoformat())
    context.log.info("elementary_alerts_sensor posted %d alerts", posted)
    return SensorResult(run_requests=[])


ALL_SENSORS: list[Any] = [
    opensky_producer_health_sensor,
    silver_aircraft_state_freshness_sensor,
    dagster_failure_to_slack_sensor,
    elementary_alerts_sensor,
]
