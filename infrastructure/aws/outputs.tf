
# Output of the s3_role ARN
output "s3_role_arn" {
  description = "The ARN of the s3_role"
  value       = aws_iam_role.s3_role.arn
}
output "s3_bucket_name" {
  description = "The name of the s3 bucket"
  value       = aws_s3_bucket.s3_bucket.bucket
}