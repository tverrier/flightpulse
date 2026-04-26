"""Asset jobs — explicit selections that get scheduled or sensor-triggered.

Selectors mirror PIPELINE.md §DAG. Gold is *lazy*: the gold-refresh job is
the only path that materializes `gold.*` assets. Parent updates do not auto-
propagate (this is the encoded defense for INCIDENTS.md #2).
"""

from __future__ import annotations

from dagster import AssetKey, AssetSelection, define_asset_job

from dagster_project.assets import (
    bts_monthly_raw,
    openflights_airlines_raw,
    openflights_airports_raw,
)
from dagster_project.constants import GROUP_GOLD, GROUP_SILVER
from dagster_project.partitions import BTS_MONTHLY_PARTITIONS

_AGG_KEY = AssetKey(["agg_carrier_otp_daily"])

# -----------------------------------------------------------------------------
# Bronze ingest jobs
# -----------------------------------------------------------------------------
bts_ingest_job = define_asset_job(
    name="bts_ingest_job",
    selection=AssetSelection.assets(bts_monthly_raw),
    partitions_def=BTS_MONTHLY_PARTITIONS,
    description="Pull one BTS month → s3 bronze. Partition-keyed.",
    tags={"warehouse": "none", "layer": "bronze"},
)

openflights_seed_job = define_asset_job(
    name="openflights_seed_job",
    selection=AssetSelection.assets(openflights_airports_raw, openflights_airlines_raw),
    description="Quarterly OpenFlights airports + airlines seed.",
    tags={"warehouse": "none", "layer": "bronze"},
)


# -----------------------------------------------------------------------------
# Silver — group-scoped selector; the translator places all silver dbt models
# in GROUP_SILVER.
# -----------------------------------------------------------------------------
silver_build_job = define_asset_job(
    name="silver_build_job",
    selection=AssetSelection.groups(GROUP_SILVER),
    description="Build all silver dbt models (incremental).",
    tags={"warehouse": "snowflake", "layer": "silver"},
)


# -----------------------------------------------------------------------------
# Gold — *lazy*. Cron-scheduled only; never auto-on-parent.
# -----------------------------------------------------------------------------
gold_refresh_job = define_asset_job(
    name="gold_refresh_job",
    selection=AssetSelection.groups(GROUP_GOLD) - AssetSelection.keys(_AGG_KEY),
    description=(
        "Rebuild gold marts (fct_flight_event + dim_*). Runs every 30 min. "
        "Excludes agg_carrier_otp_daily, which has its own daily cadence."
    ),
    tags={"warehouse": "snowflake", "layer": "gold"},
)

agg_daily_job = define_asset_job(
    name="agg_daily_job",
    selection=AssetSelection.keys(_AGG_KEY),
    description="Daily roll-up of carrier on-time performance.",
    tags={"warehouse": "snowflake", "layer": "gold"},
)


# -----------------------------------------------------------------------------
# Observability — re-run dbt tests tagged `critical` so elementary picks them
# up. We use the group selection rather than a tag selection so this stays
# robust if a model loses the `critical` tag.
# -----------------------------------------------------------------------------
elementary_job = define_asset_job(
    name="elementary_job",
    selection=AssetSelection.groups(GROUP_GOLD),
    description="Re-run dbt tests on gold so elementary captures results.",
    tags={"warehouse": "snowflake", "layer": "obs"},
)
