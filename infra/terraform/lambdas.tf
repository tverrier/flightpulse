# =============================================================================
# Observability Lambdas — feed CloudWatch metrics consumed by alarms in
# observability.tf. Two functions:
#
#   * lambda_snowflake_credits — every 30 min; publishes
#     FlightPulse/Snowflake.SnowflakeCreditsLastHour per warehouse.
#   * lambda_drift_check       — hourly; publishes
#     FlightPulse/SchemaDrift.UnreviewedDriftRows.
#
# Both share the same Snowflake credentials secret (svc_dbt user), packaged
# into the Lambda zip via `archive_file` from the source dir.
# =============================================================================

# ----- Shared Snowflake credentials secret ---------------------------------
resource "aws_secretsmanager_secret" "snowflake_obs" {
  name        = "${local.prefix}/snowflake/obs-readonly"
  description = "Snowflake credentials used by observability Lambdas (read-only on FLIGHTPULSE_OBS + SNOWFLAKE.ACCOUNT_USAGE)"
}

resource "aws_secretsmanager_secret_version" "snowflake_obs" {
  secret_id = aws_secretsmanager_secret.snowflake_obs.id
  secret_string = jsonencode({
    account   = var.snowflake_account
    user      = snowflake_user.svc_dbt.name
    password  = var.snowflake_dbt_password
    role      = snowflake_role.transformer.name
    warehouse = snowflake_warehouse.xs.name
  })
}

# ----- IAM role shared by both Lambdas -------------------------------------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "obs_lambda" {
  name               = "${local.prefix}-obs-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "obs_lambda_logs" {
  role       = aws_iam_role.obs_lambda.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "obs_lambda_inline" {
  statement {
    sid     = "ReadSecret"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.snowflake_obs.arn]
  }
  statement {
    sid     = "PutMetric"
    actions = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["FlightPulse/Snowflake", "FlightPulse/SchemaDrift"]
    }
  }
}

resource "aws_iam_role_policy" "obs_lambda_inline" {
  role   = aws_iam_role.obs_lambda.id
  policy = data.aws_iam_policy_document.obs_lambda_inline.json
}

# ----- Package + deploy: snowflake_credits ---------------------------------
data "archive_file" "snowflake_credits" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/snowflake_credits"
  output_path = "${path.module}/build/snowflake_credits.zip"
}

resource "aws_lambda_function" "snowflake_credits" {
  function_name    = "${local.prefix}-snowflake-credits"
  filename         = data.archive_file.snowflake_credits.output_path
  source_code_hash = data.archive_file.snowflake_credits.output_base64sha256
  handler          = "handler.handler"
  runtime          = "python3.11"
  role             = aws_iam_role.obs_lambda.arn
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      SNOWFLAKE_SECRET_ID = aws_secretsmanager_secret.snowflake_obs.name
      METRICS_NAMESPACE   = "FlightPulse/Snowflake"
    }
  }
}

resource "aws_cloudwatch_event_rule" "snowflake_credits" {
  name                = "${local.prefix}-snowflake-credits-30m"
  schedule_expression = "rate(30 minutes)"
}

resource "aws_cloudwatch_event_target" "snowflake_credits" {
  rule      = aws_cloudwatch_event_rule.snowflake_credits.name
  target_id = "lambda"
  arn       = aws_lambda_function.snowflake_credits.arn
}

resource "aws_lambda_permission" "snowflake_credits_eb" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.snowflake_credits.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.snowflake_credits.arn
}

# ----- Package + deploy: drift_check ---------------------------------------
data "archive_file" "drift_check" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/drift_check"
  output_path = "${path.module}/build/drift_check.zip"
}

resource "aws_lambda_function" "drift_check" {
  function_name    = "${local.prefix}-drift-check"
  filename         = data.archive_file.drift_check.output_path
  source_code_hash = data.archive_file.drift_check.output_base64sha256
  handler          = "handler.handler"
  runtime          = "python3.11"
  role             = aws_iam_role.obs_lambda.arn
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      SNOWFLAKE_SECRET_ID = aws_secretsmanager_secret.snowflake_obs.name
      METRICS_NAMESPACE   = "FlightPulse/SchemaDrift"
    }
  }
}

resource "aws_cloudwatch_event_rule" "drift_check" {
  name                = "${local.prefix}-drift-check-1h"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "drift_check" {
  rule      = aws_cloudwatch_event_rule.drift_check.name
  target_id = "lambda"
  arn       = aws_lambda_function.drift_check.arn
}

resource "aws_lambda_permission" "drift_check_eb" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.drift_check.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.drift_check.arn
}
