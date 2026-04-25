# =============================================================================
# Bootstrap stack — creates the S3 bucket and DynamoDB table that the main
# stack uses for remote state. State for THIS stack is local (committed to
# .gitignore as terraform.tfstate). Run once per AWS account.
#
#   cd infra/terraform/bootstrap
#   terraform init && terraform apply
#
# Outputs are wired into ../main.tf via the `backend "s3"` block.
# =============================================================================

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project   = "flightpulse"
      ManagedBy = "terraform-bootstrap"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "state_bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name for tf state"
  default     = "flightpulse-tfstate"
}

variable "lock_table_name" {
  type    = string
  default = "flightpulse-tflock"
}

resource "aws_s3_bucket" "tfstate" {
  bucket        = var.state_bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "tflock" {
  name         = var.lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
}

output "state_bucket_name" {
  value = aws_s3_bucket.tfstate.id
}

output "lock_table_name" {
  value = aws_dynamodb_table.tflock.name
}
