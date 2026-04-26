"""Code-location entry point — wires assets, jobs, schedules, sensors, and
resources into a single `Definitions` object exposed as `defs`.

Resource keys used across the project:

  | key              | type                  | consumed by                          |
  | ---------------- | --------------------- | ------------------------------------ |
  | dbt              | DbtCliResource        | flightpulse_dbt_assets               |
  | s3               | S3Resource            | opensky_states_raw, sensors          |
  | snowflake        | SnowflakeResource     | sensors / future ad-hoc query ops    |
  | slack            | SlackAlertResource    | sensors                              |
  | cloudwatch       | CloudWatchResource    | (reserved) producer-metric reads     |
"""

from __future__ import annotations

from dagster import Definitions

from dagster_project.assets import (
    bts_monthly_raw,
    flightpulse_dbt_assets,
    openflights_airlines_raw,
    openflights_airports_raw,
    opensky_states_raw,
)
from dagster_project.jobs import (
    agg_daily_job,
    bts_ingest_job,
    elementary_job,
    gold_refresh_job,
    openflights_seed_job,
    silver_build_job,
)
from dagster_project.resources import (
    build_cloudwatch_resource,
    build_dbt_resource,
    build_s3_resource,
    build_slack_resource,
    build_snowflake_resource,
)
from dagster_project.schedules import ALL_SCHEDULES
from dagster_project.sensors import (
    ALL_SENSORS,
    silver_aircraft_state_refresh_job,
)

defs = Definitions(
    assets=[
        # bronze
        bts_monthly_raw,
        openflights_airports_raw,
        openflights_airlines_raw,
        opensky_states_raw,
        # dbt (silver + gold)
        flightpulse_dbt_assets,
    ],
    jobs=[
        bts_ingest_job,
        openflights_seed_job,
        silver_build_job,
        gold_refresh_job,
        agg_daily_job,
        elementary_job,
        silver_aircraft_state_refresh_job,
    ],
    schedules=ALL_SCHEDULES,
    sensors=ALL_SENSORS,
    resources={
        "dbt": build_dbt_resource(),
        "s3": build_s3_resource(),
        "snowflake": build_snowflake_resource(),
        "slack": build_slack_resource(),
        "cloudwatch": build_cloudwatch_resource(),
    },
)
