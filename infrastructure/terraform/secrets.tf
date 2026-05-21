# Secrets placeholders — values populated out-of-band (never in Terraform state).
# NiFi instances fetch these at startup via the AWS Secrets Manager provider.

resource "aws_secretsmanager_secret" "salesforce" {
  name                    = "openflow/${var.environment}/salesforce"
  description             = "Salesforce OAuth credentials for NiFi ${var.environment}"
  recovery_window_in_days = var.environment == "prod" ? 30 : 0
}

resource "aws_secretsmanager_secret" "postgres" {
  name                    = "openflow/${var.environment}/postgres"
  description             = "Postgres CDC credentials for NiFi ${var.environment}"
  recovery_window_in_days = var.environment == "prod" ? 30 : 0
}

resource "aws_secretsmanager_secret" "snowflake" {
  name                    = "openflow/${var.environment}/snowflake"
  description             = "Snowflake credentials for NiFi ${var.environment}"
  recovery_window_in_days = var.environment == "prod" ? 30 : 0
}

# Placeholder versions — real values are set via AWS Console / CLI, not Terraform
resource "aws_secretsmanager_secret_version" "salesforce_placeholder" {
  secret_id = aws_secretsmanager_secret.salesforce.id
  secret_string = jsonencode({
    instance_url  = "REPLACE_ME"
    client_id     = "REPLACE_ME"
    client_secret = "REPLACE_ME"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret_version" "postgres_placeholder" {
  secret_id = aws_secretsmanager_secret.postgres.id
  secret_string = jsonencode({
    host     = "REPLACE_ME"
    port     = "5432"
    database = "REPLACE_ME"
    username = "REPLACE_ME"
    password = "REPLACE_ME"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret_version" "snowflake_placeholder" {
  secret_id = aws_secretsmanager_secret.snowflake.id
  secret_string = jsonencode({
    username = "REPLACE_ME"
    password = "REPLACE_ME"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
