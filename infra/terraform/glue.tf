# =============================================================================
# Glue catalog: separate DBs for bronze (external CSV/Parquet) and silver
# (Iceberg). Athena workgroup writes results to athena_results bucket.
# =============================================================================

resource "aws_glue_catalog_database" "bronze" {
  name        = "${local.prefix}_bronze"
  description = "Bronze (raw) S3 external tables"

  create_table_default_permission {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_glue_catalog_database" "silver" {
  name        = "${local.prefix}_silver"
  description = "Silver (Iceberg) tables"

  create_table_default_permission {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

# ----- Athena workgroup -----------------------------------------------------
resource "aws_athena_workgroup" "primary" {
  name          = "${local.prefix}"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/_athena/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    bytes_scanned_cutoff_per_query = 10737418240 # 10 GB / query cap
  }
}

# ----- Glue crawlers (optional convenience for bronze prefixes) -------------
resource "aws_iam_role" "glue_crawler" {
  name               = "${local.prefix}-glue-crawler-role"
  assume_role_policy = data.aws_iam_policy_document.trust_glue.json
}

resource "aws_iam_role_policy_attachment" "glue_crawler_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_crawler_bronze_read" {
  name = "bronze-read"
  role = aws_iam_role.glue_crawler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["s3:GetObject", "s3:ListBucket"],
        Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
      },
      {
        Effect   = "Allow",
        Action   = ["kms:Decrypt", "kms:DescribeKey"],
        Resource = aws_kms_key.s3.arn
      }
    ]
  })
}

resource "aws_glue_crawler" "bts" {
  name          = "${local.prefix}-bts-crawler"
  database_name = aws_glue_catalog_database.bronze.name
  role          = aws_iam_role.glue_crawler.arn
  schedule      = "cron(0 8 5 * ? *)" # 5th of month, 08:00 UTC, after BTS load
  s3_target {
    path = "s3://${aws_s3_bucket.raw.id}/bts/"
  }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_crawler" "opensky" {
  name          = "${local.prefix}-opensky-crawler"
  database_name = aws_glue_catalog_database.bronze.name
  role          = aws_iam_role.glue_crawler.arn
  schedule      = "cron(15 * * * ? *)" # hourly :15
  s3_target {
    path = "s3://${aws_s3_bucket.raw.id}/opensky/"
  }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_crawler" "openflights" {
  name          = "${local.prefix}-openflights-crawler"
  database_name = aws_glue_catalog_database.bronze.name
  role          = aws_iam_role.glue_crawler.arn
  schedule      = "cron(0 0 1 1/3 ? *)" # quarterly
  s3_target {
    path = "s3://${aws_s3_bucket.raw.id}/openflights/"
  }
}
