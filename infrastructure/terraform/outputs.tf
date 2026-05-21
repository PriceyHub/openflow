output "nifi_instance_id" {
  description = "NiFi EC2 instance ID"
  value       = aws_instance.nifi.id
}

output "nifi_private_ip" {
  description = "NiFi private IP address"
  value       = aws_instance.nifi.private_ip
}

output "nifi_url" {
  description = "NiFi UI URL"
  value       = "https://${aws_instance.nifi.private_ip}:8443/nifi"
}

output "nifi_registry_instance_id" {
  description = "NiFi Registry EC2 instance ID (dev only)"
  value       = var.environment == "dev" ? aws_instance.nifi_registry[0].id : "shared"
}

output "nifi_registry_url" {
  description = "NiFi Registry URL"
  value       = var.environment == "dev" ? "http://${aws_instance.nifi_registry[0].private_ip}:18080/nifi-registry" : "http://${var.nifi_registry_host}:18080/nifi-registry"
}

output "staging_bucket_name" {
  description = "S3 staging bucket name"
  value       = aws_s3_bucket.staging.id
}

output "staging_bucket_arn" {
  description = "S3 staging bucket ARN"
  value       = aws_s3_bucket.staging.arn
}

output "nifi_iam_role_arn" {
  description = "IAM role ARN attached to NiFi instances"
  value       = aws_iam_role.nifi.arn
}

output "nifi_security_group_id" {
  description = "Security group ID for NiFi instances"
  value       = aws_security_group.nifi.id
}
