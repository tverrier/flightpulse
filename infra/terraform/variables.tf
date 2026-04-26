variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type        = string
  description = "dev | prod"
  default     = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "owner_email" {
  type    = string
  default = "trentverrier7@gmail.com"
}

# ----- VPC ------------------------------------------------------------------
variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "az_count" {
  type        = number
  default     = 3
  description = "Number of AZs to span. MSK Serverless needs >=2."
}

# ----- MSK ------------------------------------------------------------------
variable "kafka_topic_opensky" {
  type    = string
  default = "opensky.states.v1"
}

# ----- Snowflake ------------------------------------------------------------
variable "snowflake_account" {
  type        = string
  description = "Snowflake account locator, e.g. xy12345.us-east-1"
}

variable "snowflake_region" {
  type    = string
  default = "AWS_US_EAST_1"
}

variable "snowflake_admin_user" {
  type        = string
  description = "ACCOUNTADMIN user used by terraform"
  sensitive   = true
}

variable "snowflake_admin_password" {
  type      = string
  sensitive = true
}

variable "snowflake_dbt_password" {
  type        = string
  description = "Password for svc_dbt user"
  sensitive   = true
}

variable "snowflake_credit_quota_monthly" {
  type        = number
  default     = 50
  description = "Monthly credit ceiling for resource monitor cm_flightpulse_monthly"
}

# ----- Alerting -------------------------------------------------------------
variable "slack_webhook_url" {
  type      = string
  default   = ""
  sensitive = true
}

# ----- Cognito --------------------------------------------------------------
variable "cognito_callback_urls" {
  type    = list(string)
  default = ["https://localhost:8501/oauth2/callback", "https://localhost:8000/oauth2/callback"]
}

# ----- Dagster Cloud --------------------------------------------------------
variable "dagster_cloud_agent_token" {
  type        = string
  description = "Token used by the Dagster Cloud Hybrid agent (per SETUP.md §6). Stored in Secrets Manager."
  sensitive   = true
  default     = ""
}

variable "dagster_cloud_url" {
  type        = string
  description = "Dagster Cloud organization URL, e.g. https://flightpulse.dagster.cloud"
  default     = ""
}

variable "dagster_cloud_deployment" {
  type    = string
  default = "prod"
}

# ----- Tagging --------------------------------------------------------------
variable "extra_tags" {
  type    = map(string)
  default = {}
}
