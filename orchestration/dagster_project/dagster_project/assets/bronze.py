"""Bronze ingestion assets.

These wrap the standalone runners under `ingestion/` so Dagster can schedule,
retry, observe, and lineage-track them. The runners themselves remain the
source of truth — Dagster invokes them in-process to keep deploy artifacts
small (no separate ECS step, no shell-out).

Per PIPELINE.md DAG, three assets land in S3 bronze:

  * `bts_monthly_raw`         — partitioned monthly, BTS On-Time CSV
  * `openflights_airports_raw`/`openflights_airlines_raw` — quarterly seed
  * `opensky_states_raw`      — produced *out-of-band* by the streaming
                                producer/consumer; modeled here as an
                                observable source asset whose freshness is
                                derived from the latest object in S3.
"""

import datetime as dt
import os
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    FreshnessPolicy,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
    observable_source_asset,
)
from dagster_aws.s3 import S3Resource

from dagster_project.constants import (
    GROUP_BRONZE,
    REPO_ROOT,
    S3_BTS_PREFIX,
    S3_OPENFLIGHTS_PREFIX,
    S3_OPENSKY_PREFIX,
    S3_RAW_BUCKET,
)
from dagster_project.partitions import BTS_MONTHLY_PARTITIONS

# -----------------------------------------------------------------------------
# Retry policies — match the table in PIPELINE.md (3x exp 1m/5m/30m for BTS,
# 2x for everything else).
# -----------------------------------------------------------------------------
_BTS_RETRY = RetryPolicy(max_retries=3, delay=60)  # HTTP-level 60→300→1800 backoff is internal to the runner
_SEED_RETRY = RetryPolicy(max_retries=2, delay=60)


# =============================================================================
# bts_monthly_raw — calls into ingestion.airbyte.run_bts_sync.main()
# =============================================================================
@asset(
    name="bts_monthly_raw",
    key_prefix=["bronze", "bts"],
    group_name=GROUP_BRONZE,
    description=(
        "BTS On-Time Performance monthly CSV landed at "
        "s3://${S3_RAW_BUCKET}/bts/ds=YYYY-MM-01/raw.csv with sidecar "
        "_manifest.json. Idempotent: rerun overwrites and short-circuits on "
        "unchanged sha256."
    ),
    partitions_def=BTS_MONTHLY_PARTITIONS,
    retry_policy=_BTS_RETRY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 35),
    compute_kind="python",
    metadata={
        "source": MetadataValue.url("https://www.transtats.bts.gov/PREZIP/"),
        "s3_prefix": MetadataValue.path(f"s3://{S3_RAW_BUCKET}/{S3_BTS_PREFIX}/"),
    },
    op_tags={"warehouse": "none", "dagster/max_runtime": str(60 * 30)},
)
def bts_monthly_raw(context: AssetExecutionContext) -> MaterializeResult:
    # Lazy import so a missing `ingestion` package doesn't break Dagster
    # code-location loading during tests.
    from ingestion.airbyte.run_bts_sync import main as run_bts_sync_main

    partition_date = dt.date.fromisoformat(context.partition_key)
    month = f"{partition_date.year}-{partition_date.month:02d}"
    bucket = os.environ.get("S3_RAW_BUCKET", S3_RAW_BUCKET)
    source_yaml = str(REPO_ROOT / "ingestion" / "airbyte" / "bts_source.yaml")

    context.log.info("running BTS sync month=%s bucket=%s", month, bucket)
    # The runner uses argparse against sys.argv; shim to pass our argv in.
    rc = _invoke_with_argv(run_bts_sync_main, [
        "--month", month,
        "--bucket", bucket,
        "--source", source_yaml,
    ])
    if rc != 0:
        raise RuntimeError(f"run_bts_sync exited rc={rc} for month={month}")

    raw_key = f"{S3_BTS_PREFIX}/ds={partition_date.isoformat()}/raw.csv"
    return MaterializeResult(
        metadata={
            "month": MetadataValue.text(month),
            "s3_uri": MetadataValue.path(f"s3://{bucket}/{raw_key}"),
            "manifest": MetadataValue.path(
                f"s3://{bucket}/{S3_BTS_PREFIX}/ds={partition_date.isoformat()}/_manifest.json"
            ),
            "runner": MetadataValue.text("ingestion.airbyte.run_bts_sync"),
        },
    )


def _invoke_with_argv(fn: Any, argv: list[str]) -> int:
    """Call a Python `main()` that uses `argparse` against process sys.argv.

    `argparse.ArgumentParser.parse_args()` defaults to `sys.argv[1:]`. We
    save/restore the slot so retries inside one process don't leak.
    """
    import sys

    saved = sys.argv
    sys.argv = [saved[0], *argv]
    try:
        return int(fn() or 0)
    finally:
        sys.argv = saved


# =============================================================================
# OpenFlights seeds — quarterly; emit two assets so dim_carrier / dim_airport
# can declare them as upstream individually.
# =============================================================================
@asset(
    name="openflights_airports_raw",
    key_prefix=["bronze", "openflights"],
    group_name=GROUP_BRONZE,
    description="OpenFlights airports.dat re-headered + uploaded to S3.",
    retry_policy=_SEED_RETRY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 100),
    compute_kind="python",
)
def openflights_airports_raw(context: AssetExecutionContext) -> MaterializeResult:
    from ingestion.seeds.load_openflights import main as load_openflights_main

    bucket = os.environ.get("S3_RAW_BUCKET", S3_RAW_BUCKET)
    ds = dt.date.today().isoformat()
    rc = load_openflights_main([
        "--bucket", bucket,
        "--prefix", S3_OPENFLIGHTS_PREFIX,
        "--ds", ds,
    ])
    if rc != 0:
        raise RuntimeError(f"load_openflights exited rc={rc}")

    key = f"{S3_OPENFLIGHTS_PREFIX}/ds={ds}/airports.csv"
    return MaterializeResult(
        metadata={
            "ds": MetadataValue.text(ds),
            "s3_uri": MetadataValue.path(f"s3://{bucket}/{key}"),
        },
    )


@asset(
    name="openflights_airlines_raw",
    key_prefix=["bronze", "openflights"],
    group_name=GROUP_BRONZE,
    description="OpenFlights airlines.dat re-headered + uploaded to S3.",
    retry_policy=_SEED_RETRY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 100),
    compute_kind="python",
)
def openflights_airlines_raw(context: AssetExecutionContext) -> MaterializeResult:
    # Both airports and airlines are produced by a single runner invocation;
    # Dagster lets us materialize both assets from one job step but to keep
    # asset-level retries cleanly isolated we just call the runner twice. The
    # runner is idempotent (sha256 check on manifest), so the second call is
    # cheap and only re-uploads if the upstream content changed since the
    # first call (typically zero rewrites).
    from ingestion.seeds.load_openflights import main as load_openflights_main

    bucket = os.environ.get("S3_RAW_BUCKET", S3_RAW_BUCKET)
    ds = dt.date.today().isoformat()
    rc = load_openflights_main([
        "--bucket", bucket,
        "--prefix", S3_OPENFLIGHTS_PREFIX,
        "--ds", ds,
    ])
    if rc != 0:
        raise RuntimeError(f"load_openflights exited rc={rc}")

    key = f"{S3_OPENFLIGHTS_PREFIX}/ds={ds}/airlines.csv"
    return MaterializeResult(
        metadata={
            "ds": MetadataValue.text(ds),
            "s3_uri": MetadataValue.path(f"s3://{bucket}/{key}"),
        },
    )


# =============================================================================
# opensky_states_raw — observable source asset.
#
# The actual writes happen out-of-band: opensky_consumer.py rolls 1-min
# Parquet files into s3://${S3_RAW_BUCKET}/opensky/ds=…/hr=…/. Dagster's job
# is to *observe* freshness so the silver dbt model can react and the
# producer-health sensor has a heartbeat to compare against.
#
# `observable_source_asset` runs the function on a sensor or schedule; it
# returns a DataVersion derived from the most-recent S3 object's last-modified
# timestamp, which downstream dbt assets see as a parent-version change.
# =============================================================================
@observable_source_asset(
    key=AssetKey(["bronze", "opensky", "opensky_states_raw"]),
    group_name=GROUP_BRONZE,
    description=(
        "Streaming OpenSky state vectors written by opensky_consumer.py. "
        "Materialized externally (ECS Fargate); Dagster observes freshness "
        "by listing the most-recent object under "
        "s3://${S3_RAW_BUCKET}/opensky/."
    ),
    metadata={
        "s3_prefix": MetadataValue.path(f"s3://{S3_RAW_BUCKET}/{S3_OPENSKY_PREFIX}/"),
        "cadence": MetadataValue.text("1-minute Parquet rolls"),
    },
)
def opensky_states_raw(context, s3: S3Resource) -> DataVersion:  # noqa: ANN001
    """Return a DataVersion = ISO timestamp of the latest opensky parquet object.

    On a fresh bucket (no objects yet) we return the literal "empty" so the
    downstream silver model still sees a stable upstream version and skips
    until data arrives.
    """
    bucket = os.environ.get("S3_RAW_BUCKET", S3_RAW_BUCKET)
    today = dt.datetime.now(dt.timezone.utc).date()
    yesterday = today - dt.timedelta(days=1)

    client = s3.get_client()
    latest_mtime: dt.datetime | None = None
    object_count = 0
    # Look at today + yesterday only — the full historical scan would be too
    # expensive on every sensor tick.
    for ds in (today.isoformat(), yesterday.isoformat()):
        prefix = f"{S3_OPENSKY_PREFIX}/ds={ds}/"
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}):
            for obj in page.get("Contents", []) or []:
                object_count += 1
                lm = obj.get("LastModified")
                if isinstance(lm, dt.datetime) and (latest_mtime is None or lm > latest_mtime):
                    latest_mtime = lm

    if latest_mtime is None:
        context.log.warning("no opensky objects under s3://%s/%s", bucket, S3_OPENSKY_PREFIX)
        return DataVersion("empty")

    context.add_output_metadata({
        "latest_object_mtime": MetadataValue.text(latest_mtime.isoformat()),
        "objects_scanned_today_plus_yesterday": MetadataValue.int(object_count),
        "lag_seconds": MetadataValue.int(
            int((dt.datetime.now(dt.timezone.utc) - latest_mtime).total_seconds())
        ),
    })
    return DataVersion(latest_mtime.isoformat())
