# =============================================================================
# Snowflake objects matching SETUP.md § 4.
#   Roles:        rl_flightpulse_admin, rl_flightpulse_transformer, rl_dashboard_ro
#   Warehouse:    wh_flightpulse_xs (XSMALL, auto-suspend 60s)
#   Databases:    flightpulse_prod, flightpulse_dev, flightpulse_obs
#   User:         svc_dbt
#   Monitor:      cm_flightpulse_monthly (50 credit cap, 80%/100% triggers)
#   Storage int:  s3_silver pointing at the silver bucket
# =============================================================================

# ----- Roles ---------------------------------------------------------------
resource "snowflake_role" "admin" {
  name    = "RL_FLIGHTPULSE_ADMIN"
  comment = "Owns FlightPulse objects"
}

resource "snowflake_role" "transformer" {
  name    = "RL_FLIGHTPULSE_TRANSFORMER"
  comment = "dbt service role"
}

resource "snowflake_role" "dashboard_ro" {
  name    = "RL_DASHBOARD_RO"
  comment = "Read-only for Streamlit dashboard"
}

# ----- Warehouse -----------------------------------------------------------
resource "snowflake_warehouse" "xs" {
  name                 = "WH_FLIGHTPULSE_XS"
  warehouse_size       = "XSMALL"
  auto_suspend         = 60
  auto_resume          = true
  initially_suspended  = true
  warehouse_type       = "STANDARD"
  comment              = "Default flightpulse compute"
}

# ----- Databases -----------------------------------------------------------
resource "snowflake_database" "prod" {
  name = "FLIGHTPULSE_PROD"
}

resource "snowflake_database" "dev" {
  name = "FLIGHTPULSE_DEV"
}

resource "snowflake_database" "obs" {
  name = "FLIGHTPULSE_OBS"
}

# ----- Schemas (mirrors dbt model layout) ----------------------------------
resource "snowflake_schema" "prod_marts" {
  database = snowflake_database.prod.name
  name     = "MARTS"
}
resource "snowflake_schema" "prod_intermediate" {
  database = snowflake_database.prod.name
  name     = "INTERMEDIATE"
}
resource "snowflake_schema" "prod_staging" {
  database = snowflake_database.prod.name
  name     = "STAGING"
}
resource "snowflake_schema" "dev_marts" {
  database = snowflake_database.dev.name
  name     = "MARTS"
}
resource "snowflake_schema" "obs_elementary" {
  database = snowflake_database.obs.name
  name     = "ELEMENTARY"
}
resource "snowflake_schema" "obs_drift" {
  database = snowflake_database.obs.name
  name     = "SCHEMA_DRIFT"
}

# ----- Grants --------------------------------------------------------------
# Use snowflake_grant_privileges_to_account_role (the supported resource as of
# provider 0.94+; the older snowflake_database_grant / warehouse_grant /
# role_grants are deprecated and removed in newer versions).

resource "snowflake_grant_privileges_to_account_role" "transformer_prod" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE", "MONITOR", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.prod.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_dev" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE", "MONITOR", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.dev.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_obs" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE", "MONITOR", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.obs.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "ro_prod" {
  account_role_name = snowflake_role.dashboard_ro.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.prod.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_wh" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.xs.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "ro_wh" {
  account_role_name = snowflake_role.dashboard_ro.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.xs.name
  }
}

# ----- User: svc_dbt -------------------------------------------------------
resource "snowflake_user" "svc_dbt" {
  name              = "SVC_DBT"
  password          = var.snowflake_dbt_password
  default_role      = snowflake_role.transformer.name
  default_warehouse = snowflake_warehouse.xs.name
  must_change_password = false
}

resource "snowflake_grant_account_role" "svc_dbt" {
  role_name = snowflake_role.transformer.name
  user_name = snowflake_user.svc_dbt.name
}

# ----- Resource monitor ----------------------------------------------------
resource "snowflake_resource_monitor" "monthly" {
  name             = "CM_FLIGHTPULSE_MONTHLY"
  credit_quota     = var.snowflake_credit_quota_monthly
  frequency        = "MONTHLY"
  start_timestamp  = "IMMEDIATELY"
  warehouses       = [snowflake_warehouse.xs.id]

  notify_triggers            = [70]
  suspend_trigger            = 80
  suspend_immediate_trigger  = 100
}

# ----- Storage integration → silver bucket --------------------------------
resource "snowflake_storage_integration" "s3_silver" {
  name    = "INT_S3_FLIGHTPULSE_SILVER"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider         = "S3"
  storage_aws_role_arn     = "arn:aws:iam::${local.account_id}:role/${local.prefix}-snowflake-storage-int"
  storage_allowed_locations = ["s3://${local.prefix}-silver-${local.bucket_suffix}/"]

  comment = "Read-only access from Snowflake to silver bucket for Iceberg ext tables"
}

# ----- External volume + Iceberg catalog integration -----------------------
resource "snowflake_external_volume" "silver" {
  name = "EV_FLIGHTPULSE_SILVER"
  storage_location {
    storage_location_name = "us-east-1"
    storage_provider      = "S3"
    storage_base_url      = "s3://${local.prefix}-silver-${local.bucket_suffix}/iceberg/"
    storage_aws_role_arn  = aws_iam_role.snowflake_storage_int.arn
    encryption {
      type     = "AWS_SSE_KMS"
      kms_key_id = aws_kms_key.s3.arn
    }
  }
}

resource "snowflake_catalog_integration" "glue" {
  name        = "CI_FLIGHTPULSE_GLUE"
  catalog_source = "GLUE"
  table_format   = "ICEBERG"

  glue_aws_role_arn = aws_iam_role.snowflake_storage_int.arn
  glue_catalog_id   = local.account_id
  glue_region       = local.region
  catalog_namespace = aws_glue_catalog_database.silver.name

  enabled = true
}
