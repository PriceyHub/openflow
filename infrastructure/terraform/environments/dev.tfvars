environment                 = "dev"
nifi_instance_type          = "t3.medium"
nifi_registry_instance_type = "t3.small"
nifi_version                = "2.0.0"
nifi_registry_version       = "2.0.0"

# Fill in after VPC creation
vpc_id             = "vpc-05954de08b894ccc9"
private_subnet_ids = ["subnet-0890dd7c01e88153b"]
public_subnet_ids  = ["subnet-0890dd7c01e88153b"]
key_pair_name      = "openflow-dev"

# Your existing RDS security group
rds_security_group_id = "sg-0c028ba9d46d9feac"

allowed_cidr_blocks = ["10.0.0.0/8"]

snowflake_account      = "cngfczx-ow26289"
snowflake_iam_user_arn = "arn:aws:iam::281525601122:user/8utp1000-s"
snowflake_external_id  = "ZM16110_SFCRole=6_zFuoyG9C5dlSTIWcX7qNyXvkxWc="
