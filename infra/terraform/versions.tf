terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.94"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
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
