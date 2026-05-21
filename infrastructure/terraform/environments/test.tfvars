environment                 = "test"
nifi_instance_type          = "t3.large"
nifi_registry_instance_type = "t3.medium"
nifi_version                = "2.0.0"
nifi_registry_version       = "2.0.0"

vpc_id             = "vpc-REPLACE"
private_subnet_ids = ["subnet-REPLACE"]
public_subnet_ids  = ["subnet-REPLACE"]
key_pair_name      = "openflow-test"

rds_security_group_id = "sg-REPLACE"

# NiFi Registry is shared — provide its IP from dev workspace output
nifi_registry_host = "REPLACE_WITH_DEV_REGISTRY_IP"

allowed_cidr_blocks = ["10.0.0.0/8"]

snowflake_account = "cngfczx-ow26289"
