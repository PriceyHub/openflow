data "aws_iam_policy_document" "nifi_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "nifi" {
  name               = "openflow-nifi-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.nifi_assume_role.json
}

data "aws_iam_policy_document" "nifi_s3" {
  statement {
    sid    = "StagingBucketAccess"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      aws_s3_bucket.staging.arn,
      "${aws_s3_bucket.staging.arn}/*"
    ]
  }
}

data "aws_iam_policy_document" "nifi_secrets" {
  statement {
    sid    = "SecretsManagerAccess"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret"
    ]
    resources = [
      "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:openflow/${var.environment}/*"
    ]
  }
}

data "aws_iam_policy_document" "nifi_ssm" {
  statement {
    sid    = "SSMCoreAccess"
    effect = "Allow"
    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath",
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
      "ec2messages:AcknowledgeMessage",
      "ec2messages:DeleteMessage",
      "ec2messages:FailMessage",
      "ec2messages:GetEndpoint",
      "ec2messages:GetMessages",
      "ec2messages:SendReply"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "nifi_s3" {
  name   = "openflow-nifi-s3-${var.environment}"
  policy = data.aws_iam_policy_document.nifi_s3.json
}

resource "aws_iam_policy" "nifi_secrets" {
  name   = "openflow-nifi-secrets-${var.environment}"
  policy = data.aws_iam_policy_document.nifi_secrets.json
}

resource "aws_iam_policy" "nifi_ssm" {
  name   = "openflow-nifi-ssm-${var.environment}"
  policy = data.aws_iam_policy_document.nifi_ssm.json
}

resource "aws_iam_role_policy_attachment" "nifi_s3" {
  role       = aws_iam_role.nifi.name
  policy_arn = aws_iam_policy.nifi_s3.arn
}

resource "aws_iam_role_policy_attachment" "nifi_secrets" {
  role       = aws_iam_role.nifi.name
  policy_arn = aws_iam_policy.nifi_secrets.arn
}

resource "aws_iam_role_policy_attachment" "nifi_ssm" {
  role       = aws_iam_role.nifi.name
  policy_arn = aws_iam_policy.nifi_ssm.arn
}

resource "aws_iam_role_policy_attachment" "nifi_ssm_core" {
  role       = aws_iam_role.nifi.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "nifi" {
  name = "openflow-nifi-${var.environment}"
  role = aws_iam_role.nifi.name
}

# ============================================================
# Snowflake → S3 storage integration role
# ============================================================

data "aws_iam_policy_document" "snowflake_s3_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_iam_user_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_s3" {
  name               = "openflow-snowflake-s3-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.snowflake_s3_assume_role.json

  tags = merge(var.tags, {
    Project     = "openflow"
    Environment = var.environment
    ManagedBy   = "terraform"
  })
}

data "aws_iam_policy_document" "snowflake_s3_access" {
  statement {
    sid    = "StagingBucketAccess"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.staging.arn,
      "${aws_s3_bucket.staging.arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "snowflake_s3" {
  name   = "snowflake-s3-staging-access"
  role   = aws_iam_role.snowflake_s3.name
  policy = data.aws_iam_policy_document.snowflake_s3_access.json
}

# ============================================================
# GitHub Actions OIDC — CI/CD deploy role
# ============================================================

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "github_actions_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:PriceyHub/openflow:*"]
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "openflow-github-actions"
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume_role.json

  tags = {
    Project   = "openflow"
    ManagedBy = "terraform"
  }
}

data "aws_iam_policy_document" "github_actions_ci" {
  statement {
    sid     = "SecretsManagerAccess"
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [
      "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:openflow/*"
    ]
  }
  statement {
    sid    = "StagingBucketAccess"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.staging.arn,
      "${aws_s3_bucket.staging.arn}/*",
    ]
  }
  statement {
    sid    = "SSMStartSession"
    effect = "Allow"
    actions = [
      "ssm:StartSession",
      "ssm:TerminateSession",
      "ssm:DescribeSessions",
    ]
    resources = [
      "arn:aws:ec2:${var.aws_region}:${data.aws_caller_identity.current.account_id}:instance/*",
      "arn:aws:ssm:${var.aws_region}::document/AWS-StartPortForwardingSession",
      "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:session/*",
    ]
  }
  statement {
    sid    = "SSMMessages"
    effect = "Allow"
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "github_actions_ci" {
  name   = "openflow-ci-permissions"
  role   = aws_iam_role.github_actions.name
  policy = data.aws_iam_policy_document.github_actions_ci.json
}
