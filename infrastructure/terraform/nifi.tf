locals {
  nifi_user_data = base64encode(templatefile("${path.module}/templates/nifi_userdata.sh.tpl", {
    environment          = var.environment
    nifi_version         = var.nifi_version
    nifi_registry_host   = var.environment == "dev" ? aws_instance.nifi_registry[0].private_ip : var.nifi_registry_host
    aws_region           = var.aws_region
    snowflake_account    = var.snowflake_account
  }))
}

resource "aws_instance" "nifi" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.nifi_instance_type
  key_name               = var.key_pair_name
  subnet_id              = var.private_subnet_ids[0]
  vpc_security_group_ids = [aws_security_group.nifi.id]
  iam_instance_profile   = aws_iam_instance_profile.nifi.name
  user_data_base64       = local.nifi_user_data

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  tags = {
    Name        = "openflow-nifi-${var.environment}"
    Environment = var.environment
  }

  lifecycle {
    ignore_changes = [user_data_base64, ami]
  }
}

resource "aws_ebs_volume" "nifi_data" {
  availability_zone = aws_instance.nifi.availability_zone
  size              = 200
  type              = "gp3"
  encrypted         = true

  tags = {
    Name = "openflow-nifi-data-${var.environment}"
  }
}

resource "aws_volume_attachment" "nifi_data" {
  device_name  = "/dev/xvdf"
  volume_id    = aws_ebs_volume.nifi_data.id
  instance_id  = aws_instance.nifi.id
  force_detach = false
}
