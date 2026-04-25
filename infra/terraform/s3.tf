# =============================================================================
# S3 buckets for the medallion lakehouse.
#
#   raw    — bronze layer; partitioned by ds=YYYY-MM-DD; 3-yr Glacier transition
#   silver — Iceberg tables (data + manifests + metadata)
#   athena_results — query results scratch space
#   logs   — bucket access logs + CloudTrail data events
# =============================================================================

# ----- Raw / Bronze ---------------------------------------------------------
resource "aws_s3_bucket" "raw" {
  bucket = "${local.prefix}-raw-${local.bucket_suffix}"
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.s3.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "tier-bts-to-glacier-3y"
    status = "Enabled"

    filter { prefix = "bts/" }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
    expiration {
      days = 1095
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "expire-opensky-90d"
    status = "Enabled"
    filter { prefix = "opensky/" }
    expiration { days = 90 }
    noncurrent_version_expiration { noncurrent_days = 7 }
  }

  rule {
    id     = "abort-mpu-7d"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_s3_bucket_logging" "raw" {
  bucket        = aws_s3_bucket.raw.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3-access/raw/"
}

# ----- Silver / Iceberg -----------------------------------------------------
resource "aws_s3_bucket" "silver" {
  bucket = "${local.prefix}-silver-${local.bucket_suffix}"
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.s3.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "expire-orphaned-iceberg-snapshots-30d"
    status = "Enabled"
    filter { prefix = "_orphaned/" }
    expiration { days = 30 }
  }

  rule {
    id     = "abort-mpu-7d"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

# ----- Athena query results -------------------------------------------------
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${local.prefix}-athena-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-30d"
    status = "Enabled"
    filter {}
    expiration { days = 30 }
  }
}

# ----- Logs bucket ----------------------------------------------------------
resource "aws_s3_bucket" "logs" {
  bucket        = "${local.prefix}-logs-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    id     = "expire-180d"
    status = "Enabled"
    filter {}
    expiration { days = 180 }
  }
}

resource "aws_s3_bucket_ownership_controls" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule { object_ownership = "BucketOwnerPreferred" }
}

resource "aws_s3_bucket_acl" "logs" {
  depends_on = [aws_s3_bucket_ownership_controls.logs]
  bucket     = aws_s3_bucket.logs.id
  acl        = "log-delivery-write"
}

# ----- KMS for S3 encryption ------------------------------------------------
resource "aws_kms_key" "s3" {
  description             = "flightpulse S3 envelope key"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "s3" {
  name          = "alias/${local.prefix}-s3"
  target_key_id = aws_kms_key.s3.key_id
}
