# NiFi Registry is deployed once (in dev env Terraform workspace) and shared
# across all environments via nifi_registry_host variable in test/prod.
resource "aws_instance" "nifi_registry" {
  count = var.environment == "dev" ? 1 : 0

  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.nifi_registry_instance_type
  key_name               = var.key_pair_name
  subnet_id              = var.private_subnet_ids[0]
  vpc_security_group_ids = [aws_security_group.nifi_registry[0].id]
  iam_instance_profile   = aws_iam_instance_profile.nifi.name

  user_data = base64encode(templatefile("${path.module}/templates/nifi_registry_userdata.sh.tpl", {
    nifi_registry_version = var.nifi_registry_version
    aws_region            = var.aws_region
  }))

  root_block_device {
    volume_size           = 50
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  tags = {
    Name        = "openflow-nifi-registry"
    Environment = "shared"
  }

  lifecycle {
    ignore_changes = [user_data, ami]
  }
}

resource "aws_ebs_volume" "nifi_registry_data" {
  count             = var.environment == "dev" ? 1 : 0
  availability_zone = aws_instance.nifi_registry[0].availability_zone
  size              = 50
  type              = "gp3"
  encrypted         = true

  tags = {
    Name = "openflow-nifi-registry-data"
  }
}

resource "aws_volume_attachment" "nifi_registry_data" {
  count        = var.environment == "dev" ? 1 : 0
  device_name  = "/dev/xvdf"
  volume_id    = aws_ebs_volume.nifi_registry_data[0].id
  instance_id  = aws_instance.nifi_registry[0].id
  force_detach = false
}
