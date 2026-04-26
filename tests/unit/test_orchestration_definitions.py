"""Smoke tests for the FlightPulse Dagster code location.

These exercise the parts of the Definitions object that don't need real AWS
or Snowflake credentials:

  * the module loads (i.e. dbt manifest bootstrap doesn't crash)
  * every job named in PIPELINE.md is registered
  * every cron schedule matches the table in PIPELINE.md
  * sensors expose the expected names + intervals
  * bronze partition def covers BTS history back to 2009-01

They depend on `dagster` + `dagster_dbt` being importable. If those packages
aren't installed in the test environment the whole module is skipped, so
`make test` on a stripped-down dev machine still passes.
"""

from __future__ import annotations

import pytest

dagster = pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")
pytest.importorskip("dagster_aws")

from dagster_project import defs  # noqa: E402
from dagster_project.partitions import BTS_MONTHLY_PARTITIONS  # noqa: E402
from dagster_project.schedules import (  # noqa: E402
    bts_monthly_schedule,
)


# -----------------------------------------------------------------------------
# Definitions surface
# -----------------------------------------------------------------------------
def test_definitions_loads():
    assert defs is not None
    repo_def = defs.get_repository_def()
    assert repo_def is not None


def test_expected_jobs_registered():
    expected = {
        "bts_ingest_job",
        "openflights_seed_job",
        "silver_build_job",
        "gold_refresh_job",
        "agg_daily_job",
        "elementary_job",
        "silver_aircraft_state_refresh_job",
    }
    job_names = {j.name for j in defs.get_repository_def().get_all_jobs()}
    missing = expected - job_names
    assert not missing, f"jobs missing from Definitions: {missing}"


# -----------------------------------------------------------------------------
# Schedules — cron strings come from PIPELINE.md DAG table.
# -----------------------------------------------------------------------------
@pytest.mark.parametrize(
    "schedule_name,expected_cron,expected_job",
    [
        ("bts_monthly_schedule",          "0 6 5 * *",   "bts_ingest_job"),
        ("openflights_quarterly_schedule","0 0 1 */3 *", "openflights_seed_job"),
        ("silver_5min_schedule",          "*/5 * * * *", "silver_build_job"),
        ("gold_half_hour_schedule",       "*/30 * * * *","gold_refresh_job"),
        ("agg_daily_schedule",            "0 3 * * *",   "agg_daily_job"),
        ("elementary_daily_schedule",     "0 4 * * *",   "elementary_job"),
    ],
)
def test_schedules_have_expected_cron(schedule_name, expected_cron, expected_job):
    repo = defs.get_repository_def()
    sched = repo.get_schedule_def(schedule_name)
    assert sched.cron_schedule == expected_cron
    assert sched.job_name == expected_job
    assert sched.execution_timezone == "UTC"


def test_bts_schedule_partition_key_is_previous_month():
    """Schedule fires on the 5th and should always target the *previous* month.

    We invoke the schedule fn directly with a context whose
    `scheduled_execution_time` is May 5 2026 → expect partition_key 2026-04-01.
    """
    import datetime as dt

    from dagster import build_schedule_context

    ctx = build_schedule_context(
        scheduled_execution_time=dt.datetime(2026, 5, 5, 6, 0, tzinfo=dt.timezone.utc)
    )
    run_request = bts_monthly_schedule(ctx)
    assert run_request.partition_key == "2026-04-01"
    assert run_request.tags["month"] == "2026-04-01"


# -----------------------------------------------------------------------------
# Sensors
# -----------------------------------------------------------------------------
@pytest.mark.parametrize(
    "sensor_name,expected_min_interval",
    [
        ("opensky_producer_health_sensor", 60),
        ("silver_aircraft_state_freshness_sensor", 60 * 5),
    ],
)
def test_sensor_intervals(sensor_name, expected_min_interval):
    repo = defs.get_repository_def()
    sensor = repo.get_sensor_def(sensor_name)
    assert sensor.minimum_interval_seconds == expected_min_interval


def test_failure_sensor_registered():
    repo = defs.get_repository_def()
    names = {s.name for s in repo.sensor_defs}
    assert "dagster_failure_to_slack_sensor" in names


# -----------------------------------------------------------------------------
# Partition definition coverage.
# -----------------------------------------------------------------------------
def test_bts_partitions_start_2009():
    import datetime as dt

    # Partition keys are "YYYY-MM-01"; first one must be 2009-01-01.
    keys = BTS_MONTHLY_PARTITIONS.get_partition_keys(
        current_time=dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc)
    )
    assert keys[0] == "2009-01-01"
    # And the most recent partition is at most "current month - 1" (end_offset=0
    # means we only enumerate partitions whose end <= current_time).
    assert keys[-1] <= "2026-05-01"


# -----------------------------------------------------------------------------
# Resource keys
# -----------------------------------------------------------------------------
def test_definitions_exposes_required_resources():
    expected = {"dbt", "s3", "snowflake", "slack", "cloudwatch"}
    actual = set(defs.resources.keys()) if hasattr(defs, "resources") else set()
    if not actual:
        # Older Definitions exposes resources via private API; fall back to
        # introspecting the repo's resource defs.
        actual = set(defs.get_repository_def()._resource_defs.keys())  # noqa: SLF001
    missing = expected - actual
    assert not missing, f"resources missing: {missing}"
