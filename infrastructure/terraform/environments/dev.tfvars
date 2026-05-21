environment                 = "dev"
nifi_instance_type          = "t3.large"
nifi_registry_instance_type = "t3.medium"
nifi_version                = "2.0.0"
nifi_registry_version       = "2.0.0"

# Fill in after VPC creation
vpc_id             = "vpc-REPLACE"
private_subnet_ids = ["subnet-REPLACE"]
public_subnet_ids  = ["subnet-REPLACE"]
key_pair_name      = "openflow-dev"

# Your existing RDS security group
rds_security_group_id = "sg-REPLACE"

allowed_cidr_blocks = ["10.0.0.0/8"]

snowflake_account = "cngfczx-ow26289"
