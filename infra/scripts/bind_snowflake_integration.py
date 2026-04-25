#!/usr/bin/env python3
"""Finalize the Snowflake↔S3 storage integration trust.

Reads `terraform output -json` from infra/terraform/, runs `DESC INTEGRATION`
in Snowflake to recover the auto-generated `STORAGE_AWS_IAM_USER_ARN` and
`STORAGE_AWS_EXTERNAL_ID`, then re-applies them to the IAM trust policy on
`flightpulse-snowflake-storage-int`.

Idempotent — safe to rerun.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import boto3
import snowflake.connector

TF_DIR = Path(__file__).resolve().parents[1] / "terraform"


def _tf_outputs() -> dict:
    raw = subprocess.check_output(
        ["terraform", "output", "-json"], cwd=TF_DIR, text=True
    )
    return {k: v["value"] for k, v in json.loads(raw).items()}


def _snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ADMIN_ROLE", "ACCOUNTADMIN"),
    )


def _describe_integration(integration: str) -> dict[str, str]:
    with _snowflake_conn() as conn, conn.cursor() as cur:
        cur.execute(f"DESC INTEGRATION {integration}")
        rows = cur.fetchall()
    out: dict[str, str] = {}
    for property_name, _, value, _ in rows:
        out[property_name] = value
    return out


def _update_trust(role_name: str, snowflake_user_arn: str, external_id: str) -> None:
    iam = boto3.client("iam")
    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": snowflake_user_arn},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
            }
        ],
    }
    iam.update_assume_role_policy(
        RoleName=role_name, PolicyDocument=json.dumps(trust)
    )


def main() -> int:
    outputs = _tf_outputs()
    integration = outputs["snowflake_storage_integration_name"]
    role_arn = outputs["snowflake_storage_integration_arn"]
    role_name = role_arn.split("/")[-1]

    desc = _describe_integration(integration)
    snowflake_user_arn = desc["STORAGE_AWS_IAM_USER_ARN"]
    external_id = desc["STORAGE_AWS_EXTERNAL_ID"]

    print(f"Snowflake → AWS principal : {snowflake_user_arn}")
    print(f"Snowflake → external_id   : {external_id}")
    print(f"Updating IAM role trust   : {role_name}")
    _update_trust(role_name, snowflake_user_arn, external_id)
    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
