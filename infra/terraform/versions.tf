terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      # 0.96+ exposes snowflake_external_volume, snowflake_catalog_integration,
      # and the new snowflake_grant_privileges_to_account_role resource that
      # supersedes the deprecated *_grant primitives.
      version = "~> 0.96"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  backend "s3" {
    # Bucket / table created by ./bootstrap. Override with -backend-config on
    # `terraform init` if you renamed them.
    bucket         = "flightpulse-tfstate"
    key            = "flightpulse/main/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "flightpulse-tflock"
    encrypt        = true
  }
}
