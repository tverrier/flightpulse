# Terraform bootstrap

One-time creation of the S3 bucket + DynamoDB lock table used as the remote
backend for the main stack. State for this stack is local — there is no
chicken-and-egg.

```bash
cd infra/terraform/bootstrap
terraform init
terraform apply -auto-approve
```

The outputs (`state_bucket_name`, `lock_table_name`) feed the
`backend "s3"` block in `../main.tf`.

Run **once per AWS account** unless rotating buckets.
