# =============================================================================
# Provider config + global locals.
# Resources live in their own .tf files (s3, msk, iam, glue, snowflake,
# cognito, cloudfront, ecs, vpc).
# =============================================================================

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "flightpulse"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = var.owner_email
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_admin_user
  password = var.snowflake_admin_password
  role     = "ACCOUNTADMIN"
  region   = var.snowflake_region
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
  partition  = data.aws_partition.current.partition

  # Resource name prefix — short, lowercase, hyphenated.
  prefix = "flightpulse"

  # S3 bucket suffix to keep names globally unique without exposing account id.
  bucket_suffix = substr(sha1("${local.account_id}-${var.environment}"), 0, 8)

  common_tags = {
    Project     = "flightpulse"
    Environment = var.environment
  }
}
