output "raw_bucket_name" {
  value = aws_s3_bucket.raw.id
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.id
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.id
}

output "logs_bucket_name" {
  value = aws_s3_bucket.logs.id
}

output "glue_db_bronze_name" {
  value = aws_glue_catalog_database.bronze.name
}

output "glue_db_name" {
  value = aws_glue_catalog_database.silver.name
}

output "msk_bootstrap_brokers" {
  value = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.this.arn
}

output "ecr_producer_repo_url" {
  value = aws_ecr_repository.opensky_producer.repository_url
}

output "ecr_consumer_repo_url" {
  value = aws_ecr_repository.opensky_consumer.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "snowflake_storage_integration_name" {
  value = snowflake_storage_integration.s3_silver.name
}

output "snowflake_storage_integration_arn" {
  value = aws_iam_role.snowflake_storage_int.arn
}

output "cognito_user_pool_id" {
  value = aws_cognito_user_pool.this.id
}

output "cognito_user_pool_client_id" {
  value = aws_cognito_user_pool_client.web.id
}

output "cloudfront_domain_name" {
  value = aws_cloudfront_distribution.web.domain_name
}

output "vpc_id" {
  value = aws_vpc.this.id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}
