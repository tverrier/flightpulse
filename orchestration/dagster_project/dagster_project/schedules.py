"""Cron schedules + bronze auto-materialize wiring.

Cron strings come straight from PIPELINE.md §DAG (the "Schedule / sensor"
column). All schedules are UTC. Each schedule attaches to a job defined in
`jobs.py`.
"""

from __future__ import annotations

import datetime as dt

from dagster import (
    DefaultScheduleStatus,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    schedule,
)

from dagster_project.jobs import (
    agg_daily_job,
    bts_ingest_job,
    elementary_job,
    gold_refresh_job,
    openflights_seed_job,
    silver_build_job,
)
from dagster_project.partitions import BTS_MONTHLY_PARTITIONS

# -----------------------------------------------------------------------------
# BTS — published by transtats around the 5th of each month for prior month.
# Cron `0 6 5 * *` UTC. We compute the partition_key as "previous month".
# -----------------------------------------------------------------------------
@schedule(
    cron_schedule="0 6 5 * *",
    job=bts_ingest_job,
    name="bts_monthly_schedule",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)
def bts_monthly_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    today = context.scheduled_execution_time.date() if context.scheduled_execution_time else dt.date.today()
    # Walk back to the first of the previous month.
    first_of_this_month = today.replace(day=1)
    prev_month_last_day = first_of_this_month - dt.timedelta(days=1)
    target = prev_month_last_day.replace(day=1).isoformat()
    return RunRequest(
        run_key=f"bts-{target}",
        partition_key=target,
        tags={"trigger": "bts_monthly_schedule", "month": target},
    )


# -----------------------------------------------------------------------------
# OpenFlights — `0 0 1 */3 *` (1st of Jan/Apr/Jul/Oct, 00:00 UTC).
# -----------------------------------------------------------------------------
openflights_quarterly_schedule = ScheduleDefinition(
    name="openflights_quarterly_schedule",
    cron_schedule="0 0 1 */3 *",
    job=openflights_seed_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)


# -----------------------------------------------------------------------------
# Silver — runs every 5 min. The opensky observable source asset's freshness
# triggers the auto-materialize sensor on a 5-min poll for silver_aircraft_state;
# the BTS-fed silver_flight_event is also picked up as a no-op when nothing
# upstream has changed.
# -----------------------------------------------------------------------------
silver_5min_schedule = ScheduleDefinition(
    name="silver_5min_schedule",
    cron_schedule="*/5 * * * *",
    job=silver_build_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)


# -----------------------------------------------------------------------------
# Gold — every 30 min, *lazy*. PIPELINE.md "lazy; cron */30 * * * *".
# -----------------------------------------------------------------------------
gold_half_hour_schedule = ScheduleDefinition(
    name="gold_half_hour_schedule",
    cron_schedule="*/30 * * * *",
    job=gold_refresh_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)


# -----------------------------------------------------------------------------
# Daily aggregates — 03:00 UTC.
# -----------------------------------------------------------------------------
agg_daily_schedule = ScheduleDefinition(
    name="agg_daily_schedule",
    cron_schedule="0 3 * * *",
    job=agg_daily_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)


# -----------------------------------------------------------------------------
# Elementary tests — 04:00 UTC, after the daily agg.
# -----------------------------------------------------------------------------
elementary_daily_schedule = ScheduleDefinition(
    name="elementary_daily_schedule",
    cron_schedule="0 4 * * *",
    job=elementary_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)


# Convenience export — the Definitions object iterates this list.
ALL_SCHEDULES = [
    bts_monthly_schedule,
    openflights_quarterly_schedule,
    silver_5min_schedule,
    gold_half_hour_schedule,
    agg_daily_schedule,
    elementary_daily_schedule,
]

# Re-export so tests can introspect the partition def from a single import.
__all__ = [
    "ALL_SCHEDULES",
    "BTS_MONTHLY_PARTITIONS",
    "agg_daily_schedule",
    "bts_monthly_schedule",
    "elementary_daily_schedule",
    "gold_half_hour_schedule",
    "openflights_quarterly_schedule",
    "silver_5min_schedule",
]
