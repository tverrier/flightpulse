"""Schema-drift staleness metric publisher.

Counts rows in FLIGHTPULSE_OBS.SCHEMA_DRIFT.BTS_COLUMN_DRIFT older than 7 d
that have no `reviewed_at` and emits `UnreviewedDriftRows` to
FlightPulse/SchemaDrift. The CloudWatch alarm in observability.tf pages on
>0 — encoded defense for INCIDENTS.md #1 (silent NULL coercion).

Mirrors the singular dbt test `assert_no_unreviewed_drift_over_7_days.sql` in
the AWS metric plane so SREs see drift without polling Snowflake.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

SECRET_ID = os.environ["SNOWFLAKE_SECRET_ID"]
NAMESPACE = os.environ.get("METRICS_NAMESPACE", "FlightPulse/SchemaDrift")

_QUERY = """
select count(*) as unreviewed
from FLIGHTPULSE_OBS.SCHEMA_DRIFT.BTS_COLUMN_DRIFT
where reviewed_at is null
  and detected_at < dateadd(day, -7, current_timestamp())
""".strip()


def _connect() -> Any:
    import snowflake.connector  # type: ignore[import-not-found]

    sm = boto3.client("secretsmanager")
    secret = json.loads(sm.get_secret_value(SecretId=SECRET_ID)["SecretString"])
    return snowflake.connector.connect(
        account=secret["account"],
        user=secret["user"],
        password=secret["password"],
        role=secret.get("role", "RL_FLIGHTPULSE_TRANSFORMER"),
        warehouse=secret.get("warehouse", "WH_FLIGHTPULSE_XS"),
        database="FLIGHTPULSE_OBS",
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ARG001
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(_QUERY)
        row = cur.fetchone() or (0,)
        count = int(row[0] or 0)

    boto3.client("cloudwatch").put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[
            {
                "MetricName": "UnreviewedDriftRows",
                "Value": float(count),
                "Unit": "Count",
            }
        ],
    )
    LOGGER.info("UnreviewedDriftRows=%d", count)
    return {"unreviewed": count}
