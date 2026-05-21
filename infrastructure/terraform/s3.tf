resource "aws_s3_bucket" "staging" {
  bucket = "openflow-staging-${var.environment}-eu-west-2"
}

resource "aws_s3_bucket_versioning" "staging" {
  bucket = aws_s3_bucket.staging.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "staging" {
  bucket                  = aws_s3_bucket.staging.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id

  rule {
    id     = "expire-staged-files"
    status = "Enabled"

    filter {
      prefix = "staged/"
    }

    expiration {
      days = 3
    }
  }

  rule {
    id     = "expire-error-files"
    status = "Enabled"

    filter {
      prefix = "errors/"
    }

    expiration {
      days = 30
    }
  }
}

# Snowflake needs to be able to read from this bucket via storage integration
# The IAM trust policy is created after running: CREATE STORAGE INTEGRATION in Snowflake
resource "aws_iam_role" "snowflake_s3_access" {
  name = "openflow-snowflake-s3-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "sts:ExternalId" = "OPENFLOW_${upper(var.environment)}" }
      }
    }]
  })
}

resource "aws_iam_role_policy" "snowflake_s3_access" {
  name = "s3-read-staging"
  role = aws_iam_role.snowflake_s3_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.staging.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.staging.arn
      }
    ]
  })
}
