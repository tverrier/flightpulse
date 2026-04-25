# =============================================================================
# MSK Serverless cluster — IAM-auth Kafka. No broker mgmt; pay per throughput.
# Uses VPC private subnets and the dedicated MSK security group.
# Topic creation (opensky.states.v1, 6 partitions, 24h retention) is performed
# by the producer container on first start via admin client (idempotent).
# =============================================================================

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = "${local.prefix}-msk"

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = {
    Name = "${local.prefix}-msk"
  }
}

# ----- Log group for application-level kafka client logs --------------------
resource "aws_cloudwatch_log_group" "msk_clients" {
  name              = "/flightpulse/msk-clients"
  retention_in_days = 14
}

# ----- CloudWatch alarms wired to producer SLOs (PIPELINE.md) ---------------
resource "aws_cloudwatch_metric_alarm" "producer_silent" {
  alarm_name          = "${local.prefix}-producer-silent-60s"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "MessagesPublished"
  namespace           = "FlightPulse/OpenSkyProducer"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_description   = "Producer published zero messages for 2 consecutive minutes (Incident #3 signature)"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  alarm_name          = "${local.prefix}-consumer-lag-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "EstimatedMaxTimeLag"
  namespace           = "AWS/Kafka"
  period              = 300
  statistic           = "Maximum"
  threshold           = 30
  alarm_description   = "Consumer lag > 30s for 5 min on any partition"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    "Cluster Name" = aws_msk_serverless_cluster.this.cluster_name
  }
}

# ----- SNS → Slack ----------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${local.prefix}-alerts"
}

resource "aws_sns_topic_subscription" "slack" {
  count     = var.slack_webhook_url == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "https"
  endpoint  = var.slack_webhook_url
}
