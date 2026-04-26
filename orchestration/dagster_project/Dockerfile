# =============================================================================
# Dagster code-location image. Built by `dagster-cloud serverless deploy`
# (Serverless) or by the Hybrid agent against the registry referenced in
# dagster_cloud.yaml. Mirrors the local `make dagster-up` invocation:
#
#   - install requirements.txt (project-wide deps incl. dbt-snowflake,
#     dagster-aws, dagster-dbt, snowflake-connector-python, elementary)
#   - install the dagster_project package itself
#   - copy the dbt project so DBT_PROFILES_DIR / DBT_PROJECT_DIR resolve to
#     /opt/dagster/app/dbt (matching constants.py)
# =============================================================================
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    DBT_PROJECT_DIR=/opt/dagster/app/dbt \
    DBT_PROFILES_DIR=/opt/dagster/app/dbt \
    DBT_TARGET=prod

WORKDIR /opt/dagster/app

# System deps for snowflake-connector-python's cryptography build + pyarrow.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/*

# Layer 1 — project requirements (changes least often).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# Layer 2 — the dagster project package.
COPY orchestration/dagster_project /opt/dagster/app/orchestration/dagster_project
RUN pip install --no-cache-dir /opt/dagster/app/orchestration/dagster_project

# Layer 3 — dbt project + ingestion package (assets import ingestion modules).
COPY dbt /opt/dagster/app/dbt
COPY ingestion /opt/dagster/app/ingestion

# dagster.yaml lives in DAGSTER_HOME at runtime.
RUN mkdir -p $DAGSTER_HOME
COPY orchestration/dagster_project/dagster.yaml $DAGSTER_HOME/dagster.yaml

WORKDIR /opt/dagster/app/orchestration/dagster_project
