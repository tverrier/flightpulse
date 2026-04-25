# =============================================================================
# ECS Fargate cluster + ECR repos for the streaming containers and API.
# Task definitions and services are created in Phase 3 once the Dockerfiles
# exist; this file provisions the cluster, ECR, log groups, exec role, and the
# ALB that CloudFront points to.
# =============================================================================

resource "aws_ecs_cluster" "this" {
  name = "${local.prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }
}

# ----- ECR repos -----------------------------------------------------------
resource "aws_ecr_repository" "opensky_producer" {
  name                 = "${local.prefix}/opensky-producer"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.s3.arn
  }
}

resource "aws_ecr_repository" "opensky_consumer" {
  name                 = "${local.prefix}/opensky-consumer"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
}

resource "aws_ecr_repository" "api" {
  name                 = "${local.prefix}/api"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
}

resource "aws_ecr_repository" "dashboard" {
  name                 = "${local.prefix}/dashboard"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
}

resource "aws_ecr_lifecycle_policy" "keep_last_20" {
  for_each   = toset([aws_ecr_repository.opensky_producer.name, aws_ecr_repository.opensky_consumer.name, aws_ecr_repository.api.name, aws_ecr_repository.dashboard.name])
  repository = each.value
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 20 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = { type = "expire" }
    }]
  })
}

# ----- Task execution role (pull image, write logs) ------------------------
resource "aws_iam_role" "ecs_task_exec" {
  name               = "${local.prefix}-ecs-task-exec"
  assume_role_policy = data.aws_iam_policy_document.trust_ecs_tasks.json
}

resource "aws_iam_role_policy_attachment" "ecs_task_exec" {
  role       = aws_iam_role.ecs_task_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ----- CloudWatch log groups ----------------------------------------------
resource "aws_cloudwatch_log_group" "producer" {
  name              = "/flightpulse/ecs/opensky-producer"
  retention_in_days = 14
}
resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/flightpulse/ecs/opensky-consumer"
  retention_in_days = 14
}
resource "aws_cloudwatch_log_group" "api" {
  name              = "/flightpulse/ecs/api"
  retention_in_days = 14
}
resource "aws_cloudwatch_log_group" "dashboard" {
  name              = "/flightpulse/ecs/dashboard"
  retention_in_days = 14
}

# ----- ALB shared by dashboard + api --------------------------------------
resource "aws_security_group" "alb" {
  name        = "${local.prefix}-alb-sg"
  description = "ALB ingress (CloudFront)"
  vpc_id      = aws_vpc.this.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "alb_to_tasks" {
  type                     = "ingress"
  from_port                = 8000
  to_port                  = 8501
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.alb.id
  security_group_id        = aws_security_group.ecs_tasks.id
  description              = "ALB → Fargate tasks (api 8000, dashboard 8501)"
}

resource "aws_lb" "web" {
  name               = "${local.prefix}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id
}

resource "aws_lb_target_group" "dashboard" {
  name        = "${local.prefix}-dash-tg"
  port        = 8501
  protocol    = "HTTP"
  vpc_id      = aws_vpc.this.id
  target_type = "ip"

  health_check {
    path                = "/_stcore/health"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_target_group" "api" {
  name        = "${local.prefix}-api-tg"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.this.id
  target_type = "ip"

  health_check {
    path                = "/healthz"
    matcher             = "200"
    interval            = 15
    timeout             = 3
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.web.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.dashboard.arn
  }
}

resource "aws_lb_listener_rule" "api" {
  listener_arn = aws_lb_listener.http.arn
  priority     = 10

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }

  condition {
    path_pattern { values = ["/api/*", "/predict", "/healthz"] }
  }

  condition {
    http_header {
      http_header_name = "X-CloudFront-Secret"
      values           = [random_password.cf_origin_secret.result]
    }
  }
}
