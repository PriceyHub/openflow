resource "aws_security_group" "nifi" {
  name        = "openflow-nifi-${var.environment}"
  description = "Security group for NiFi ${var.environment} instance"
  vpc_id      = var.vpc_id

  ingress {
    description = "NiFi HTTPS UI"
    from_port   = 8443
    to_port     = 8443
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  ingress {
    description = "NiFi cluster communication"
    from_port   = 11443
    to_port     = 11443
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "NiFi Site-to-Site"
    from_port   = 10000
    to_port     = 10000
    protocol    = "tcp"
    self        = true
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "openflow-nifi-${var.environment}"
  }
}

resource "aws_security_group" "nifi_registry" {
  count = var.environment == "dev" ? 1 : 0

  name        = "openflow-nifi-registry"
  description = "Security group for NiFi Registry (shared)"
  vpc_id      = var.vpc_id

  ingress {
    description     = "NiFi Registry HTTP from NiFi SG"
    from_port       = 18080
    to_port         = 18080
    protocol        = "tcp"
    security_groups = [aws_security_group.nifi.id]
  }

  ingress {
    description = "NiFi Registry HTTP from allowed CIDRs"
    from_port   = 18080
    to_port     = 18080
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "openflow-nifi-registry"
  }
}

# Allow NiFi to reach RDS Postgres
resource "aws_security_group_rule" "nifi_to_rds" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.nifi.id
  security_group_id        = var.rds_security_group_id
  description              = "NiFi ${var.environment} to RDS Postgres"
}
