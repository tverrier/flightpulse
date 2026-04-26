# =============================================================================
# Dagster Cloud wiring (per SETUP.md §6 + §8).
#
# Stores the agent token in Secrets Manager so a Hybrid agent on this account
# (or any future ECS task) can pull it without having the token live in the
# task definition. If `var.dagster_cloud_agent_token` is empty (e.g. dev
# accounts not using Dagster Cloud), the secret is still created with a
# placeholder value so the rest of the stack remains plan-clean — operators
# rotate the value out-of-band via `aws secretsmanager update-secret`.
#
# An ECR repo for the code-location image is provisioned here so the
# dagster-cloud-deploy GitHub Actions workflow has a stable target. The agent
# itself is deployed out-of-band (Helm chart on EKS or `dagster-cloud agent`
# on ECS) — Dagster does not publish a Terraform-native agent module yet.
# =============================================================================

resource "aws_secretsmanager_secret" "dagster_cloud_agent_token" {
  name        = "${local.prefix}/dagster-cloud/agent-token"
  description = "Dagster Cloud Hybrid agent token. Rotate via Dagster Cloud UI → settings → tokens."
}

resource "aws_secretsmanager_secret_version" "dagster_cloud_agent_token" {
  secret_id     = aws_secretsmanager_secret.dagster_cloud_agent_token.id
  secret_string = var.dagster_cloud_agent_token == "" ? "PLACEHOLDER_ROTATE_ME" : var.dagster_cloud_agent_token
}

resource "aws_ecr_repository" "dagster_code_location" {
  name                 = "${local.prefix}/dagster-code-location"
  image_tag_mutability = "MUTABLE" # Dagster Cloud retags `latest` per deploy
  image_scanning_configuration { scan_on_push = true }
}

# Optional Hybrid-agent role — assumed by the agent's task to pull the token,
# read SSM params, and access the same Snowflake creds the obs Lambdas use.
data "aws_iam_policy_document" "dagster_agent_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dagster_agent" {
  name               = "${local.prefix}-dagster-agent"
  assume_role_policy = data.aws_iam_policy_document.dagster_agent_assume.json
}

data "aws_iam_policy_document" "dagster_agent_inline" {
  statement {
    sid     = "ReadDagsterAndSnowflakeSecrets"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      aws_secretsmanager_secret.dagster_cloud_agent_token.arn,
      aws_secretsmanager_secret.snowflake_obs.arn,
    ]
  }
  statement {
    sid     = "PullCodeLocationImages"
    actions = [
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
    ]
    resources = ["*"]
  }
  statement {
    sid     = "ReadS3Raw"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
    ]
  }
  statement {
    sid     = "PutMetric"
    actions = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["FlightPulse/Dagster"]
    }
  }
}

resource "aws_iam_role_policy" "dagster_agent_inline" {
  role   = aws_iam_role.dagster_agent.id
  policy = data.aws_iam_policy_document.dagster_agent_inline.json
}

# ----- Outputs consumed by infra/scripts and the Actions workflow ----------
output "dagster_code_location_repo" {
  value       = aws_ecr_repository.dagster_code_location.repository_url
  description = "Push target for the dagster_cloud.yaml `registry` field."
}

output "dagster_agent_role_arn" {
  value       = aws_iam_role.dagster_agent.arn
  description = "Role to attach to the Hybrid agent's ECS task."
}

output "dagster_cloud_agent_token_secret_arn" {
  value       = aws_secretsmanager_secret.dagster_cloud_agent_token.arn
  description = "Secrets Manager ARN holding the Hybrid agent token."
}
