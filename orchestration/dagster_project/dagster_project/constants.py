"""Project-wide constants for the Dagster code location.

Everything that depends on filesystem layout, env-var contracts, or shared
keys is centralized here so other modules don't drift.
"""

from __future__ import annotations

import os
from pathlib import Path

# Absolute path to the dbt/ directory at the repo root. We resolve up from this
# file rather than $CWD so `dagster dev` works regardless of where it was run.
REPO_ROOT: Path = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR: Path = REPO_ROOT / "dbt"
DBT_PROFILES_DIR: Path = REPO_ROOT / "dbt"

# dbt target — `prod` writes to FLIGHTPULSE_PROD; CI/local override via env.
DBT_TARGET: str = os.environ.get("DBT_TARGET", "prod")

# S3 bronze layout (mirrors PIPELINE.md §Ingestion).
S3_RAW_BUCKET: str = os.environ.get("S3_RAW_BUCKET", "flightpulse-raw")
S3_BTS_PREFIX: str = "bts"
S3_OPENSKY_PREFIX: str = "opensky"
S3_OPENFLIGHTS_PREFIX: str = "openflights"

# Producer heartbeat path inside ECS task — sensor reads CloudWatch metric
# in prod, but for local docker-compose we fall back to file mtime.
PRODUCER_HEARTBEAT_PATH: str = os.environ.get(
    "PRODUCER_HEARTBEAT_PATH", "/tmp/opensky_producer.heartbeat"
)

# CloudWatch namespace the streaming producer/consumer emit under.
CW_NAMESPACE: str = os.environ.get(
    "CLOUDWATCH_NAMESPACE", "FlightPulse/Streaming"
)

# Slack webhook used by the failure sensor. Optional — sensor no-ops if unset.
SLACK_WEBHOOK_URL: str = os.environ.get("SLACK_WEBHOOK_URL", "")

# Dagster group names — keep in sync with the DAG diagram in PIPELINE.md.
GROUP_BRONZE: str = "bronze_ingest"
GROUP_SILVER: str = "silver_dbt"
GROUP_GOLD: str = "gold_dbt"
GROUP_OBS: str = "observability"
