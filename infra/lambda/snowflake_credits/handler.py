"""Snowflake credit-burn metric publisher.

Runs every 30 minutes via EventBridge; queries
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY for the last hour and
publishes a `SnowflakeCreditsLastHour` metric to FlightPulse/Snowflake (one
datapoint per warehouse). Wired to the Incident-#2 CloudWatch alarm in
infra/terraform/observability.tf.

ACCOUNT_USAGE has up to 45 min of latency, so we look back 75 min and sum to
keep the metric monotonic and lossy-but-safe.
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
NAMESPACE = os.environ.get("METRICS_NAMESPACE", "FlightPulse/Snowflake")

_QUERY = """
select
    warehouse_name,
    sum(credits_used) as credits
from snowflake.account_usage.warehouse_metering_history
where end_time > dateadd(minute, -75, current_timestamp())
group by warehouse_name
""".strip()


def _connect() -> Any:
    import snowflake.connector  # type: ignore[import-not-found]

    sm = boto3.client("secretsmanager")
    secret = json.loads(sm.get_secret_value(SecretId=SECRET_ID)["SecretString"])
    return snowflake.connector.connect(
        account=secret["account"],
        user=secret["user"],
        password=secret["password"],
        role=secret.get("role", "ACCOUNTADMIN"),
        warehouse=secret.get("warehouse", "WH_FLIGHTPULSE_XS"),
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ARG001
    cw = boto3.client("cloudwatch")
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(_QUERY)
        rows = cur.fetchall() or []

    if not rows:
        LOGGER.info("no warehouse usage in last 75 min")
        return {"datapoints": 0}

    metric_data = [
        {
            "MetricName": "SnowflakeCreditsLastHour",
            "Value": float(credits or 0.0),
            "Unit": "Count",
            "Dimensions": [{"Name": "Warehouse", "Value": str(name)}],
        }
        for name, credits in rows
    ]
    cw.put_metric_data(Namespace=NAMESPACE, MetricData=metric_data)
    LOGGER.info("published %d credit datapoints", len(metric_data))
    return {"datapoints": len(metric_data)}
