# =============================================================================
# CloudWatch metric alarms — Phase 7.
#
# Three classes of alarm wired to the same SNS topic (aws_sns_topic.alerts in
# msk.tf) so every page lands in #flightpulse-alerts:
#
#   1. Producer / consumer health    — Incident #3 (TCP half-close) signature.
#      The producer-silent + consumer-lag alarms already live in msk.tf; this
#      file extends with DLQ rate, consumer-error rate, and a heartbeat
#      *staleness* alarm on `MessagesPublished` over a 5-min window.
#
#   2. MERGE-loop credit guard       — Incident #2.
#      The producer publishes Snowflake credit usage to a custom metric every
#      30 min from a Lambda that queries SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_-
#      METERING_HISTORY (see infra/lambda/snowflake_credits/). Alarm fires if
#      the rolling 1-hour average exceeds a threshold that would burn through
#      the monthly resource-monitor cap in <72 h. We *also* keep the Snowflake
#      resource-monitor suspend trigger (cm_flightpulse_monthly @ 80%) — the
#      CloudWatch alarm is the early-warning belt; the resource monitor is
#      the suspenders.
#
#   3. Schema-drift staleness        — Incident #1.
#      A small Lambda (infra/lambda/drift_check) runs hourly, queries
#      FLIGHTPULSE_OBS.SCHEMA_DRIFT.BTS_COLUMN_DRIFT for any rows where
#      reviewed_at IS NULL AND detected_at < now() - interval 7 days, and
#      publishes a `UnreviewedDriftRows` metric. Alarm fires on >0.
#
# We *do not* alarm on every dbt test failure here — that is elementary's job
# (FLIGHTPULSE_OBS.elementary.alerts → Dagster sensor → Slack). CloudWatch
# alarms are reserved for AWS-side signals and the credit guard.
# =============================================================================

# -----------------------------------------------------------------------------
# 1. Streaming health
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "consumer_dlq_rate" {
  alarm_name          = "${local.prefix}-consumer-dlq-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "MessagesDLQ"
  namespace           = "FlightPulse/OpenSkyConsumer"
  period              = 300
  statistic           = "Sum"
  threshold           = 50
  treat_missing_data  = "notBreaching"
  alarm_description   = "Consumer dead-lettered >50 messages in 5 min — likely a poison-pill or Avro/JSON drift"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  dimensions = {
    Component = "consumer"
  }
}

resource "aws_cloudwatch_metric_alarm" "consumer_error_rate" {
  alarm_name          = "${local.prefix}-consumer-error-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ConsumerError"
  namespace           = "FlightPulse/OpenSkyConsumer"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
  treat_missing_data  = "notBreaching"
  alarm_description   = "Consumer recorded >5 errors/min for 3 consecutive minutes"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    Component = "consumer"
  }
}

resource "aws_cloudwatch_metric_alarm" "producer_heartbeat_stale" {
  # Sister to producer_silent in msk.tf: producer_silent triggers fast (2 min,
  # 0 messages); this one triggers on a *longer* window before paging on-call,
  # so transient OpenSky API blips don't wake anyone up.
  alarm_name          = "${local.prefix}-producer-heartbeat-stale-5m"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "MessagesPublished"
  namespace           = "FlightPulse/OpenSkyProducer"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_description   = "Producer heartbeat stale for 5 min — Incident #3 signature (TCP half-close). Page on-call."
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  dimensions = {
    Component = "producer"
  }
}

resource "aws_cloudwatch_metric_alarm" "producer_fetch_fatal" {
  alarm_name          = "${local.prefix}-producer-fetch-fatal"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "FetchFatal"
  namespace           = "FlightPulse/OpenSkyProducer"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_description   = "Producer recorded a fatal OpenSky fetch error (4xx other than 429, malformed payload, or auth failure)"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    Component = "producer"
  }
}

# -----------------------------------------------------------------------------
# 2. MERGE-loop credit guard (Incident #2)
#
# A scheduled Lambda (see infra/lambda/snowflake_credits) emits the
# `SnowflakeCreditsLastHour` metric every 30 min. Threshold reasoning:
#
#   monthly cap = var.snowflake_credit_quota_monthly  (default 50 credits)
#   72-hour burn-through threshold = monthly cap / (30 * 24 / 72)
#                                  = monthly cap * 72 / 720
#                                  = monthly cap * 0.10
#                                  = 5 credits/hour for the default cap
#
# We round up to 6 to give slight headroom for the daily agg + gold refresh
# baseline. If the CloudWatch alarm fires, gold has likely tipped from
# `incremental` back into a rebuild loop — page on-call before the resource
# monitor's hard suspend at 80%.
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "snowflake_credit_burn" {
  alarm_name          = "${local.prefix}-snowflake-credit-burn"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "SnowflakeCreditsLastHour"
  namespace           = "FlightPulse/Snowflake"
  period              = 1800
  statistic           = "Maximum"
  threshold           = max(ceil(var.snowflake_credit_quota_monthly * 0.10), 6)
  treat_missing_data  = "notBreaching"
  alarm_description   = <<-EOT
    Snowflake hourly credit burn projects to consume the monthly cap in <72 h.
    Likely cause: gold materialization regressed from incremental → table, or a
    streaming asset is punching MERGEs (Incident #2). Suspend the offending
    warehouse and inspect QUERY_HISTORY for the past 30 min.
  EOT
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  dimensions = {
    Warehouse = "WH_FLIGHTPULSE_XS"
  }
}

# -----------------------------------------------------------------------------
# 3. Schema-drift staleness (Incident #1)
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "unreviewed_drift" {
  alarm_name          = "${local.prefix}-unreviewed-schema-drift"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "UnreviewedDriftRows"
  namespace           = "FlightPulse/SchemaDrift"
  period              = 3600
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_description   = "BTS_COLUMN_DRIFT has rows older than 7 d with no reviewer — Incident #1 prevention"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# -----------------------------------------------------------------------------
# Dashboard — single pane of glass for the SRE on-call.
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_dashboard" "flightpulse" {
  dashboard_name = "${local.prefix}-overview"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric",
        x      = 0,
        y      = 0,
        width  = 12,
        height = 6,
        properties = {
          title  = "Producer throughput",
          region = local.region,
          stat   = "Sum",
          period = 60,
          metrics = [
            ["FlightPulse/OpenSkyProducer", "MessagesPublished", "Component", "producer"],
            [".", "MessagesFailed", ".", "."],
            [".", "FetchTransient", ".", "."],
            [".", "FetchFatal", ".", "."],
          ]
        }
      },
      {
        type   = "metric",
        x      = 12,
        y      = 0,
        width  = 12,
        height = 6,
        properties = {
          title  = "Consumer throughput",
          region = local.region,
          stat   = "Sum",
          period = 60,
          metrics = [
            ["FlightPulse/OpenSkyConsumer", "RowsFlushed", "Component", "consumer"],
            [".", "ConsumerError", ".", "."],
            [".", "MessagesDLQ", ".", "."],
          ]
        }
      },
      {
        type   = "metric",
        x      = 0,
        y      = 6,
        width  = 12,
        height = 6,
        properties = {
          title  = "Snowflake credit burn (last hour)",
          region = local.region,
          stat   = "Maximum",
          period = 1800,
          metrics = [
            ["FlightPulse/Snowflake", "SnowflakeCreditsLastHour", "Warehouse", "WH_FLIGHTPULSE_XS"],
          ],
          annotations = {
            horizontal = [
              {
                value = max(ceil(var.snowflake_credit_quota_monthly * 0.10), 6),
                label = "alarm threshold"
              }
            ]
          }
        }
      },
      {
        type   = "metric",
        x      = 12,
        y      = 6,
        width  = 12,
        height = 6,
        properties = {
          title  = "MSK consumer lag",
          region = local.region,
          stat   = "Maximum",
          period = 300,
          metrics = [
            ["AWS/Kafka", "EstimatedMaxTimeLag", "Cluster Name", aws_msk_serverless_cluster.this.cluster_name],
          ]
        }
      },
    ]
  })
}
