data "aws_caller_identity" "current" {}
resource "random_pet" "suffix" {
  length = 2
}

resource "aws_s3_bucket" "s3_bucket" {
  bucket = "masterclass-demo-${random_pet.suffix.id}"
  tags = {
    Name        = "masterclass"
    Environment = "DEV"
  }
}

resource "aws_iam_policy" "bucket_policy" {
  name        = "masterclass-demo-${random_pet.suffix.id}-s3-policy"
  description = "Policy for bucket masterclass-demo"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "s3:PutObject",
              "s3:GetObject",
              "s3:GetObjectVersion",
              "s3:DeleteObject",
              "s3:DeleteObjectVersion"
            ],
            "Resource": "${aws_s3_bucket.s3_bucket.arn}/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "${aws_s3_bucket.s3_bucket.arn}",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "*"
                    ]
                }
            }
        }
    ]
}
EOF
}

resource "aws_iam_role" "s3_role" {
  name = "masterclass-demo-s3-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "0000"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "s3_role_attachment" {
  policy_arn = aws_iam_policy.bucket_policy.arn
  role       = aws_iam_role.s3_role.name
}
