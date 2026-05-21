#!/bin/bash
set -euo pipefail

NIFI_REGISTRY_VERSION="${nifi_registry_version}"
AWS_REGION="${aws_region}"
REGISTRY_HOME="/opt/nifi-registry/nifi-registry-current"
REGISTRY_DATA="/data/nifi-registry"

if ! blkid /dev/xvdf; then
  mkfs.xfs /dev/xvdf
fi
mkdir -p "$REGISTRY_DATA"
mount /dev/xvdf "$REGISTRY_DATA"
echo "/dev/xvdf $REGISTRY_DATA xfs defaults,nofail 0 2" >> /etc/fstab

dnf install -y java-21-amazon-corretto-headless wget unzip git

mkdir -p /opt/nifi-registry
cd /opt/nifi-registry
wget -q "https://downloads.apache.org/nifi/$NIFI_REGISTRY_VERSION/nifi-registry-$NIFI_REGISTRY_VERSION-bin.zip"
unzip -q "nifi-registry-$NIFI_REGISTRY_VERSION-bin.zip"
ln -sfn "nifi-registry-$NIFI_REGISTRY_VERSION" nifi-registry-current

useradd -r -s /sbin/nologin nifi-registry
mkdir -p "$REGISTRY_DATA"/{database,flow_storage,providers}
chown -R nifi-registry:nifi-registry /opt/nifi-registry "$REGISTRY_DATA"

PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

cat > "$REGISTRY_HOME/conf/nifi-registry.properties" <<EOF
nifi.registry.web.http.host=$PRIVATE_IP
nifi.registry.web.http.port=18080
nifi.registry.db.directory=$REGISTRY_DATA/database
nifi.registry.flow.provider.id=fileProvider
EOF

cat > "$REGISTRY_HOME/conf/providers.xml" <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<providers>
  <flowPersistenceProvider>
    <class>org.apache.nifi.registry.provider.flow.git.GitFlowPersistenceProvider</class>
    <property name="Flow Storage Directory">$REGISTRY_DATA/flow_storage</property>
    <property name="Remote To Push">origin</property>
    <property name="Remote Access User">openflow-registry</property>
    <property name="Remote Access Password">REPLACE_WITH_GH_TOKEN</property>
  </flowPersistenceProvider>
  <extensionBundleProvider>
    <class>org.apache.nifi.registry.provider.extension.FileSystemBundleProvider</class>
    <property name="Extension Bundle Storage Directory">$REGISTRY_DATA/extensions</property>
  </extensionBundleProvider>
  <eventHookProvider>
    <class>org.apache.nifi.registry.provider.hook.ScriptEventHookProvider</class>
    <property name="Script Path"></property>
  </eventHookProvider>
</providers>
EOF

cat > /etc/systemd/system/nifi-registry.service <<EOF
[Unit]
Description=Apache NiFi Registry
After=network.target

[Service]
Type=forking
User=nifi-registry
Group=nifi-registry
ExecStart=$REGISTRY_HOME/bin/nifi-registry.sh start
ExecStop=$REGISTRY_HOME/bin/nifi-registry.sh stop
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable nifi-registry
systemctl start nifi-registry
