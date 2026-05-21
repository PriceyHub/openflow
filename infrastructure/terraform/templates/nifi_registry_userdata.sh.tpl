#!/bin/bash
set -euo pipefail

NIFI_REGISTRY_VERSION="${nifi_registry_version}"

# Format and mount data volume
if ! blkid /dev/xvdf; then
  mkfs.xfs /dev/xvdf
fi
mkdir -p /data/nifi-registry
mount /dev/xvdf /data/nifi-registry
echo "/dev/xvdf /data/nifi-registry xfs defaults,nofail 0 2" >> /etc/fstab

mkdir -p /data/nifi-registry/{database,flow_storage,extensions}

dnf install -y docker
systemctl start docker
systemctl enable docker

docker run -d --name nifi-registry \
  --restart unless-stopped \
  -p 18080:18080 \
  -v /data/nifi-registry/database:/opt/nifi-registry/nifi-registry-current/database \
  -v /data/nifi-registry/flow_storage:/opt/nifi-registry/nifi-registry-current/flow_storage \
  -v /data/nifi-registry/extensions:/opt/nifi-registry/nifi-registry-current/extension_bundles \
  "apache/nifi-registry:1.27.0"

echo "NiFi Registry container started"
