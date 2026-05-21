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
      "s3:GetBucketLocation"
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
