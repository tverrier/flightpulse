"""Dagster resources shared across assets/jobs/sensors.

Concrete configuration is pulled from environment variables (per SETUP.md §8)
inside the build helpers — assets and sensors only see typed `ConfigurableResource`
instances. Every resource is constructed lazily so that local `pytest` runs
that import `Definitions` do not require AWS/Snowflake credentials at import
time.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3
import requests
from botocore.config import Config as BotoConfig
from dagster import ConfigurableResource, EnvVar, InitResourceContext
from dagster_aws.s3 import S3Resource
from dagster_dbt import DbtCliResource

from dagster_project.constants import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    DBT_TARGET,
    SLACK_WEBHOOK_URL,
)

LOGGER = logging.getLogger("flightpulse.dagster.resources")


# -----------------------------------------------------------------------------
# Snowflake — thin wrapper around snowflake-connector-python so assets can
# execute ad-hoc SELECTs (used by the gold-freshness sensor and by the
# elementary report job). dbt itself uses its own DbtCliResource.
# -----------------------------------------------------------------------------
class SnowflakeResource(ConfigurableResource):
    """Pluggable Snowflake connection backed by env vars.

    Tests may swap this out with a stubbed subclass that overrides
    `query_one` / `execute`.
    """

    account: str = EnvVar("SNOWFLAKE_ACCOUNT")
    user: str = EnvVar("SNOWFLAKE_USER")
    password: str = EnvVar("SNOWFLAKE_PASSWORD")
    role: str = EnvVar("SNOWFLAKE_ROLE")
    warehouse: str = EnvVar("SNOWFLAKE_WAREHOUSE")
    database: str = EnvVar("SNOWFLAKE_DATABASE")
    schema_: str = "MARTS"

    def _connect(self) -> Any:
        # Imported lazily — keeps Dagster code-location loading fast and
        # avoids a hard dep when running unit tests with this resource stubbed.
        import snowflake.connector  # type: ignore[import-not-found]

        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            role=self.role,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema_,
        )

    def query_one(self, sql: str, params: tuple[Any, ...] | None = None) -> tuple[Any, ...] | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())
            return int(cur.rowcount or 0)


# -----------------------------------------------------------------------------
# Slack — fire-and-forget incoming webhook. No-op if SLACK_WEBHOOK_URL is unset
# so dev installs aren't forced to provision a webhook.
# -----------------------------------------------------------------------------
class SlackAlertResource(ConfigurableResource):
    webhook_url: str = ""
    timeout_seconds: float = 5.0

    def setup_for_execution(self, context: InitResourceContext) -> None:
        if not self.webhook_url:
            context.log.warning(
                "SlackAlertResource initialized without webhook_url — "
                "alerts will be logged locally only."
            )

    def post(self, text: str, *, channel: str | None = None) -> bool:
        if not self.webhook_url:
            LOGGER.info("[slack-noop] %s", text)
            return False
        body: dict[str, Any] = {"text": text}
        if channel:
            body["channel"] = channel
        try:
            resp = requests.post(
                self.webhook_url,
                data=json.dumps(body),
                headers={"Content-Type": "application/json"},
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            LOGGER.error("slack post failed: %s", exc)
            return False
        if resp.status_code >= 300:
            LOGGER.error("slack non-2xx: %s %s", resp.status_code, resp.text)
            return False
        return True


# -----------------------------------------------------------------------------
# CloudWatch — used by the OpenSky producer-health sensor. Falls back to a
# stub `get_metric_statistics` that returns no datapoints when AWS creds are
# absent (so `dagster dev` works locally).
# -----------------------------------------------------------------------------
class CloudWatchResource(ConfigurableResource):
    region_name: str = EnvVar("AWS_REGION")
    namespace: str = "FlightPulse/Streaming"

    def client(self) -> Any:
        cfg = BotoConfig(retries={"max_attempts": 3, "mode": "standard"})
        return boto3.client(
            "cloudwatch",
            region_name=self.region_name or "us-east-1",
            config=cfg,
        )


# -----------------------------------------------------------------------------
# Builders — call from `definitions.py`.
# -----------------------------------------------------------------------------
def build_dbt_resource() -> DbtCliResource:
    """DbtCliResource pinned to the repo dbt project + target.

    `profiles_dir` matches what the Makefile sets so behavior is identical
    whether dbt is run by Dagster or by hand.
    """
    return DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=str(DBT_PROFILES_DIR),
        target=DBT_TARGET,
    )


def build_s3_resource() -> S3Resource:
    return S3Resource(
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
    )


def build_snowflake_resource() -> SnowflakeResource:
    # Field names are read from EnvVar() at runtime; this just constructs.
    return SnowflakeResource()


def build_slack_resource() -> SlackAlertResource:
    return SlackAlertResource(webhook_url=SLACK_WEBHOOK_URL)


def build_cloudwatch_resource() -> CloudWatchResource:
    return CloudWatchResource()
