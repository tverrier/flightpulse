# Setup

End-to-end, ~60 min on a fresh laptop.

## 1. CLI Tools (macOS + Linux)

```bash
# macOS via Homebrew
brew install git python@3.11 awscli terraform docker snowflake-snowsql jq make

# Ubuntu/Debian
sudo apt update && sudo apt install -y git python3.11 python3.11-venv python3-pip make jq unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscli.zip && unzip awscli.zip && sudo ./aws/install
sudo snap install terraform --classic
curl -fsSL https://get.docker.com | sh
```

Verify:

```bash
git --version && python3.11 --version && aws --version && terraform -version && docker --version
```

## 2. Python Project

```bash
git clone https://github.com/<you>/flightpulse.git && cd flightpulse
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt`: `dbt-snowflake`, `dagster`, `dagster-dbt`, `dagster-aws`, `confluent-kafka`, `pyiceberg[s3,glue]`, `snowflake-connector-python`, `streamlit`, `fastapi`, `uvicorn`, `lightgbm`, `elementary-data[snowflake]`, `boto3`, `pyyaml`, `pandas`, `pyarrow`, `requests`.

## 3. AWS Account

Region: `us-east-1`. Services to enable: S3, IAM, MSK, Glue, Athena, KMS, CloudWatch, Cognito, CloudFront, Cost Explorer, ECS, ECR.

Create IAM user `flightpulse-deployer` for Terraform with this policy (least-privilege for the project scope):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*", "glue:*", "kafka:*", "kafka-cluster:*",
        "iam:CreateRole", "iam:DeleteRole", "iam:GetRole",
        "iam:PutRolePolicy", "iam:AttachRolePolicy", "iam:DetachRolePolicy",
        "iam:PassRole", "iam:CreatePolicy", "iam:DeletePolicy",
        "iam:ListPolicies", "iam:CreateInstanceProfile",
        "kms:*", "logs:*", "cloudwatch:*", "athena:*",
        "cognito-idp:*", "cloudfront:*",
        "ec2:Describe*", "ec2:CreateVpc*", "ec2:CreateSubnet*",
        "ec2:CreateSecurityGroup*", "ec2:Authorize*", "ec2:CreateRouteTable*",
        "ecs:*", "ecr:*",
        "dynamodb:CreateTable", "dynamodb:DescribeTable",
        "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"
      ],
      "Resource": "*"
    }
  ]
}
```

Runtime roles (created by Terraform, do not click-ops):

| Role | Purpose |
|---|---|
| `flightpulse-airbyte-role` | Read source secrets, write `s3://flightpulse-raw/bts/*` |
| `flightpulse-stream-role` | Produce/consume MSK, write `s3://flightpulse-raw/opensky/*` |
| `flightpulse-dagster-role` | Orchestrate; assumed by Dagster Cloud agent |
| `flightpulse-snowflake-storage-int` | Snowflake → S3 read for external tables |

## 4. Snowflake Account

Sign up at `https://signup.snowflake.com/` (Standard tier, AWS, us-east-1).

Run as ACCOUNTADMIN:

```sql
CREATE ROLE rl_flightpulse_admin;
CREATE ROLE rl_flightpulse_transformer;
CREATE ROLE rl_dashboard_ro;

CREATE WAREHOUSE wh_flightpulse_xs
  WITH WAREHOUSE_SIZE='XSMALL'
  AUTO_SUSPEND=60 AUTO_RESUME=TRUE
  INITIALLY_SUSPENDED=TRUE;

CREATE DATABASE flightpulse_prod;
CREATE DATABASE flightpulse_dev;
CREATE DATABASE flightpulse_obs;

GRANT USAGE ON DATABASE flightpulse_prod TO ROLE rl_flightpulse_transformer;
GRANT ALL ON DATABASE flightpulse_prod TO ROLE rl_flightpulse_transformer;
GRANT USAGE ON WAREHOUSE wh_flightpulse_xs TO ROLE rl_flightpulse_transformer, rl_dashboard_ro;

CREATE USER svc_dbt PASSWORD='<rotate-me>'
  DEFAULT_ROLE=rl_flightpulse_transformer
  DEFAULT_WAREHOUSE=wh_flightpulse_xs;
GRANT ROLE rl_flightpulse_transformer TO USER svc_dbt;

CREATE RESOURCE MONITOR cm_flightpulse_monthly
  WITH CREDIT_QUOTA=50 FREQUENCY=MONTHLY START_TIMESTAMP=IMMEDIATELY
  TRIGGERS
    ON 80 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;
ALTER WAREHOUSE wh_flightpulse_xs SET RESOURCE_MONITOR = cm_flightpulse_monthly;
```

Storage integration is created by Terraform; `make snowflake-bind-integration` finalizes it.

## 5. OpenSky Account

Register at `https://opensky-network.org/`. Save username + password — anonymous works but is rate-limited to once/10s; registered raises throughput.

## 6. Dagster Cloud

Sign up at `https://dagster.cloud/signup` (free Solo tier). Create deployment `flightpulse-prod`. Copy the agent token; you'll plug it into Terraform.

## 7. Terraform Walkthrough

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in values

terraform init
# Expected: "Terraform has been successfully initialized!"

terraform plan -out=tfplan
# Expected: "Plan: ~85 to add, 0 to change, 0 to destroy."

terraform apply tfplan
# Expected: 5–8 minutes; outputs include msk_bootstrap_brokers,
# raw_bucket_name, silver_bucket_name, glue_db_name,
# snowflake_storage_integration_arn.
```

After apply:

```bash
make snowflake-bind-integration   # runs DESC INTEGRATION + grants from tf outputs
```

## 8. Environment Variables

Copy `.env.example` to `.env` and fill in:

| Variable | Purpose | Where to get it |
|---|---|---|
| `AWS_REGION` | Default region | always `us-east-1` |
| `AWS_ACCESS_KEY_ID` | Deployer creds | IAM user `flightpulse-deployer` |
| `AWS_SECRET_ACCESS_KEY` | Deployer creds | same |
| `S3_RAW_BUCKET` | Bronze bucket | tf output `raw_bucket_name` |
| `S3_SILVER_BUCKET` | Silver bucket | tf output `silver_bucket_name` |
| `GLUE_DATABASE` | Glue catalog DB | tf output `glue_db_name` |
| `MSK_BOOTSTRAP` | Kafka brokers | tf output `msk_bootstrap_brokers` |
| `KAFKA_TOPIC_OPENSKY` | Topic name | always `opensky.states.v1` |
| `OPENSKY_USER` | Auth | OpenSky account |
| `OPENSKY_PASS` | Auth | OpenSky account |
| `SNOWFLAKE_ACCOUNT` | Account locator | Snowflake URL prefix |
| `SNOWFLAKE_USER` | Service user | `svc_dbt` |
| `SNOWFLAKE_PASSWORD` | Service password | from §4 |
| `SNOWFLAKE_ROLE` | Default role | `rl_flightpulse_transformer` |
| `SNOWFLAKE_WAREHOUSE` | Default WH | `wh_flightpulse_xs` |
| `SNOWFLAKE_DATABASE` | Database | `flightpulse_prod` |
| `DAGSTER_CLOUD_AGENT_TOKEN` | Agent auth | Dagster Cloud UI |
| `SLACK_WEBHOOK_URL` | Alerts | Slack incoming webhook |
| `BTS_DOWNLOAD_URL` | BTS endpoint | const, see `ingestion/airbyte/bts_source.yaml` |

## 9. First End-to-End Run

```bash
make stream-up                      # producer + consumer in docker-compose
sleep 60                            # let some events flow
make seed-openflights
make backfill MONTH=2024-01         # ~10 min
dbt deps && dbt build --target prod
make dagster-up
make dashboard
make api
```

## 10. Per-Layer Validation

```bash
# Bronze: did files land?
aws s3 ls s3://$S3_RAW_BUCKET/bts/ds=2024-01-01/ --human-readable
aws s3 ls s3://$S3_RAW_BUCKET/opensky/ds=$(date -u +%Y-%m-%d)/ | head

# Silver: row counts via Athena
aws athena start-query-execution \
  --query-string "SELECT count(*) FROM flightpulse_silver.flight_event WHERE flight_date='2024-01-15'" \
  --result-configuration "OutputLocation=s3://$S3_SILVER_BUCKET/_athena/" \
  --work-group primary

# Gold: dim + fact completeness
snowsql -q "SELECT count(*) FROM flightpulse_prod.marts.fct_flight_event WHERE flight_date='2024-01-15';"
snowsql -q "SELECT count(*) FROM flightpulse_prod.marts.dim_carrier;"

# dbt tests
dbt test --select tag:critical
```

If any validation returns 0 or fails, see [PIPELINE.md § Failure Runbook](./PIPELINE.md#failure-runbook).
