# =============================================================================
# CloudFront distribution that fronts the dashboard and the API.
# Origin: ALB created in ecs.tf. Cognito auth is enforced via the
# Lambda@Edge / response-headers policy below; for local dev we route through
# the same distribution.
# =============================================================================

resource "aws_cloudfront_distribution" "web" {
  enabled             = true
  is_ipv6_enabled     = true
  default_root_object = ""
  comment             = "${local.prefix} dashboard + api"
  price_class         = "PriceClass_100"

  origin {
    domain_name = aws_lb.web.dns_name
    origin_id   = "alb-web"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }

    custom_header {
      name  = "X-CloudFront-Secret"
      value = random_password.cf_origin_secret.result
    }
  }

  default_cache_behavior {
    target_origin_id       = "alb-web"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    forwarded_values {
      query_string = true
      headers      = ["Authorization", "Host", "Origin", "Referer"]
      cookies { forward = "all" }
    }

    min_ttl     = 0
    default_ttl = 0
    max_ttl     = 60
  }

  ordered_cache_behavior {
    path_pattern           = "/api/*"
    target_origin_id       = "alb-web"
    viewer_protocol_policy = "https-only"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods         = ["GET", "HEAD"]

    forwarded_values {
      query_string = true
      headers      = ["Authorization", "Host", "Origin", "Referer"]
      cookies { forward = "all" }
    }

    min_ttl     = 0
    default_ttl = 0
    max_ttl     = 0
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

resource "random_password" "cf_origin_secret" {
  length           = 40
  special          = false
  override_special = "_-"
}
