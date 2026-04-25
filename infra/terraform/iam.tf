# =============================================================================
# Runtime IAM roles per SETUP.md § 3.
#   flightpulse-airbyte-role          batch ingestion to bronze
#   flightpulse-stream-role           ECS Fargate producer/consumer + MSK
#   flightpulse-dagster-role          orchestrator
#   flightpulse-snowflake-storage-int Snowflake → S3 read for ext tables
# =============================================================================

# ----- Trust policies -------------------------------------------------------
data "aws_iam_policy_document" "trust_ecs_tasks" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "trust_glue" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

# ----- Shared bronze-write inline policy ------------------------------------
data "aws_iam_policy_document" "bronze_rw" {
  statement {
    sid       = "BronzeRW"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${aws_s3_bucket.raw.arn}/*"]
  }
  statement {
    sid       = "BronzeListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn]
  }
  statement {
    sid     = "KMSForBronze"
    actions = ["kms:GenerateDataKey", "kms:Decrypt", "kms:Encrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.s3.arn]
  }
}

data "aws_iam_policy_document" "silver_rw" {
  statement {
    sid       = "SilverRW"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${aws_s3_bucket.silver.arn}/*"]
  }
  statement {
    sid       = "SilverList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.silver.arn]
  }
  statement {
    sid     = "KMSForSilver"
    actions = ["kms:GenerateDataKey", "kms:Decrypt", "kms:Encrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.s3.arn]
  }
}

data "aws_iam_policy_document" "msk_iam_auth" {
  statement {
    sid     = "ConnectAndDescribe"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:AlterCluster",
      "kafka-cluster:DescribeClusterDynamicConfiguration",
    ]
    resources = [aws_msk_serverless_cluster.this.arn]
  }
  statement {
    sid     = "TopicRW"
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:DescribeTopicDynamicConfiguration",
      "kafka-cluster:AlterTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData",
      "kafka-cluster:DeleteTopic",
    ]
    resources = ["arn:${local.partition}:kafka:${local.region}:${local.account_id}:topic/${aws_msk_serverless_cluster.this.cluster_name}/*"]
  }
  statement {
    sid     = "GroupRW"
    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]
    resources = ["arn:${local.partition}:kafka:${local.region}:${local.account_id}:group/${aws_msk_serverless_cluster.this.cluster_name}/*"]
  }
}

# =============================================================================
# 1. flightpulse-airbyte-role
# =============================================================================
resource "aws_iam_role" "airbyte" {
  name               = "${local.prefix}-airbyte-role"
  assume_role_policy = data.aws_iam_policy_document.trust_ecs_tasks.json
}

resource "aws_iam_role_policy" "airbyte_bronze" {
  name   = "bronze-rw"
  role   = aws_iam_role.airbyte.id
  policy = data.aws_iam_policy_document.bronze_rw.json
}

resource "aws_iam_role_policy_attachment" "airbyte_logs" {
  role       = aws_iam_role.airbyte.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

# =============================================================================
# 2. flightpulse-stream-role
# =============================================================================
resource "aws_iam_role" "stream" {
  name               = "${local.prefix}-stream-role"
  assume_role_policy = data.aws_iam_policy_document.trust_ecs_tasks.json
}

resource "aws_iam_role_policy" "stream_bronze" {
  name   = "bronze-rw"
  role   = aws_iam_role.stream.id
  policy = data.aws_iam_policy_document.bronze_rw.json
}

resource "aws_iam_role_policy" "stream_msk" {
  name   = "msk-iam-auth"
  role   = aws_iam_role.stream.id
  policy = data.aws_iam_policy_document.msk_iam_auth.json
}

resource "aws_iam_role_policy" "stream_metrics" {
  name = "cloudwatch-publish"
  role = aws_iam_role.stream.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["cloudwatch:PutMetricData"]
      Resource = "*"
      Condition = {
        StringEquals = {
          "cloudwatch:namespace" = ["FlightPulse/OpenSkyProducer", "FlightPulse/OpenSkyConsumer"]
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "stream_logs" {
  role       = aws_iam_role.stream.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

# =============================================================================
# 3. flightpulse-dagster-role  (assumed by Dagster Cloud agent on Fargate)
# =============================================================================
resource "aws_iam_role" "dagster" {
  name               = "${local.prefix}-dagster-role"
  assume_role_policy = data.aws_iam_policy_document.trust_ecs_tasks.json
}

resource "aws_iam_role_policy" "dagster_buckets" {
  name = "lakehouse-rw"
  role = aws_iam_role.dagster.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
        Resource = [
          aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.silver.arn, "${aws_s3_bucket.silver.arn}/*",
        ]
      },
      {
        Effect   = "Allow",
        Action   = ["glue:*", "athena:*"],
        Resource = "*"
      },
      {
        Effect   = "Allow",
        Action   = ["kms:GenerateDataKey", "kms:Decrypt", "kms:Encrypt", "kms:DescribeKey"],
        Resource = aws_kms_key.s3.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dagster_logs" {
  role       = aws_iam_role.dagster.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

# =============================================================================
# 4. flightpulse-snowflake-storage-int  (assumed by Snowflake)
#    Trust is set to Snowflake's IAM principal after `make snowflake-bind-integration`.
#    Initially trusts the AWS account so terraform apply succeeds; the bind
#    script narrows it to the Snowflake-issued external_id + ARN.
# =============================================================================
data "aws_iam_policy_document" "trust_snowflake" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [snowflake_storage_integration.s3_silver.storage_aws_iam_user_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [snowflake_storage_integration.s3_silver.storage_aws_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_storage_int" {
  name               = "${local.prefix}-snowflake-storage-int"
  assume_role_policy = data.aws_iam_policy_document.trust_snowflake.json
}

resource "aws_iam_role_policy" "snowflake_silver_read" {
  name = "silver-read"
  role = aws_iam_role.snowflake_storage_int.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:GetObject", "s3:GetObjectVersion"],
        Resource = ["${aws_s3_bucket.silver.arn}/*"]
      },
      {
        Effect = "Allow",
        Action = ["s3:ListBucket", "s3:GetBucketLocation"],
        Resource = [aws_s3_bucket.silver.arn]
      },
      {
        Effect   = "Allow",
        Action   = ["kms:Decrypt", "kms:DescribeKey"],
        Resource = aws_kms_key.s3.arn
      }
    ]
  })
}
