#!/bin/bash
set -euo pipefail

ENVIRONMENT="${environment}"
AWS_REGION="${aws_region}"
NIFI_VERSION="${nifi_version}"
NIFI_REGISTRY_HOST="${nifi_registry_host}"

# Format and mount data volume
if ! blkid /dev/xvdf; then
  mkfs.xfs /dev/xvdf
fi
mkdir -p /data/nifi
mount /dev/xvdf /data/nifi
echo "/dev/xvdf /data/nifi xfs defaults,nofail 0 2" >> /etc/fstab

mkdir -p /data/nifi/{logs,database_repository,flowfile_repository,content_repository,provenance_repository,state,lib_custom}

# Install Docker
dnf install -y docker
systemctl start docker
systemctl enable docker

# Download JDBC drivers (mounted into container)
mkdir -p /data/nifi/lib_custom
curl -fsSL -o /data/nifi/lib_custom/snowflake-jdbc.jar \
  "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.22.1/snowflake-jdbc-3.22.1.jar"
curl -fsSL -o /data/nifi/lib_custom/postgresql.jar \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"

# Fetch NiFi admin password from Secrets Manager
NIFI_PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id "openflow/$ENVIRONMENT/nifi-admin" \
  --region "$AWS_REGION" \
  --query SecretString \
  --output text 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('password','openflow_admin_2026'))" 2>/dev/null || echo "openflow_admin_2026")

docker run -d --name nifi \
  --restart unless-stopped \
  -p 8443:8443 \
  -v /data/nifi/logs:/opt/nifi/nifi-current/logs \
  -v /data/nifi/database_repository:/opt/nifi/nifi-current/database_repository \
  -v /data/nifi/flowfile_repository:/opt/nifi/nifi-current/flowfile_repository \
  -v /data/nifi/content_repository:/opt/nifi/nifi-current/content_repository \
  -v /data/nifi/provenance_repository:/opt/nifi/nifi-current/provenance_repository \
  -v /data/nifi/state:/opt/nifi/nifi-current/state \
  -v /data/nifi/lib_custom:/opt/nifi/nifi-current/lib/custom \
  -e NIFI_WEB_HTTPS_HOST=0.0.0.0 \
  -e SINGLE_USER_CREDENTIALS_USERNAME=admin \
  -e SINGLE_USER_CREDENTIALS_PASSWORD="$NIFI_PASSWORD" \
  "apache/nifi:${nifi_version}"

echo "NiFi container started"
