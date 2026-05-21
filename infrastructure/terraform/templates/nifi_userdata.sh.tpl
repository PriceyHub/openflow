#!/bin/bash
set -euo pipefail

ENVIRONMENT="${environment}"
NIFI_VERSION="${nifi_version}"
NIFI_REGISTRY_HOST="${nifi_registry_host}"
AWS_REGION="${aws_region}"
NIFI_HOME="/opt/nifi/nifi-current"
NIFI_DATA="/data/nifi"

# Format and mount data volume
if ! blkid /dev/xvdf; then
  mkfs.xfs /dev/xvdf
fi
mkdir -p "$NIFI_DATA"
mount /dev/xvdf "$NIFI_DATA"
echo "/dev/xvdf $NIFI_DATA xfs defaults,nofail 0 2" >> /etc/fstab

# Install dependencies
dnf install -y java-21-amazon-corretto-headless wget unzip

# Download and install NiFi
mkdir -p /opt/nifi
cd /opt/nifi
wget -q "https://downloads.apache.org/nifi/$NIFI_VERSION/nifi-$NIFI_VERSION-bin.zip"
unzip -q "nifi-$NIFI_VERSION-bin.zip"
ln -sfn "nifi-$NIFI_VERSION" nifi-current

# Download Snowflake JDBC driver
mkdir -p "$NIFI_HOME/lib/custom"
wget -q -O "$NIFI_HOME/lib/custom/snowflake-jdbc.jar" \
  "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.4/snowflake-jdbc-3.14.4.jar"

# Create nifi user
useradd -r -s /sbin/nologin nifi
chown -R nifi:nifi /opt/nifi "$NIFI_DATA"

# Configure NiFi directories on data volume
mkdir -p "$NIFI_DATA"/{database_repository,flowfile_repository,content_repository,provenance_repository,state,logs}
chown -R nifi:nifi "$NIFI_DATA"

# Fetch credentials from Secrets Manager
SF_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id "openflow/$ENVIRONMENT/salesforce" \
  --region "$AWS_REGION" \
  --query SecretString \
  --output text)

PG_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id "openflow/$ENVIRONMENT/postgres" \
  --region "$AWS_REGION" \
  --query SecretString \
  --output text)

SF_SECRET_SNOW=$(aws secretsmanager get-secret-value \
  --secret-id "openflow/$ENVIRONMENT/snowflake" \
  --region "$AWS_REGION" \
  --query SecretString \
  --output text)

# Write NiFi bootstrap properties
cat > "$NIFI_HOME/conf/bootstrap.conf" <<EOF
java=java
run.as=nifi
lib.dir=./lib
conf.dir=./conf
graceful.shutdown.seconds=20
java.arg.2=-Xms2g
java.arg.3=-Xmx4g
java.arg.14=-Djava.awt.headless=true
EOF

# Write core nifi.properties
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

cat > "$NIFI_HOME/conf/nifi.properties" <<EOF
nifi.flow.configuration.file=$NIFI_DATA/flow.xml.gz
nifi.flow.configuration.archive.enabled=true
nifi.flow.configuration.archive.dir=$NIFI_DATA/archive
nifi.database.directory=$NIFI_DATA/database_repository
nifi.flowfile.repository.directory=$NIFI_DATA/flowfile_repository
nifi.content.repository.directory.default=$NIFI_DATA/content_repository
nifi.provenance.repository.directory.default=$NIFI_DATA/provenance_repository
nifi.state.management.config.file=./conf/state-management.xml
nifi.state.management.provider.local=local-provider

nifi.web.https.host=$PRIVATE_IP
nifi.web.https.port=8443
nifi.web.http.host=
nifi.web.http.port=

nifi.security.keystore=./conf/keystore.jks
nifi.security.keystoreType=JKS
nifi.security.keystorePasswd=changeme_gen_on_deploy
nifi.security.keyPasswd=changeme_gen_on_deploy
nifi.security.truststore=./conf/truststore.jks
nifi.security.truststoreType=JKS
nifi.security.truststorePasswd=changeme_gen_on_deploy

nifi.cluster.is.node=false
nifi.zookeeper.connect.string=

nifi.registry.flow.registry.url=http://$NIFI_REGISTRY_HOST:18080

nifi.sensitive.props.key=openflow_$ENVIRONMENT_sensitive_key
nifi.sensitive.props.algorithm=NIFI_PBKDF2_AES_GCM_256
EOF

chown -R nifi:nifi "$NIFI_HOME/conf"

# Generate self-signed TLS (replace with ACM/proper cert in prod)
cd "$NIFI_HOME"
"$NIFI_HOME/bin/nifi-toolkit.sh" tls standalone \
  --hostnames "$PRIVATE_IP" \
  --clientCertDn "CN=nifi-admin, OU=openflow" \
  --outputDirectory /tmp/tls-gen \
  2>/dev/null || true

cp /tmp/tls-gen/nifi-cert.pem "$NIFI_HOME/conf/" 2>/dev/null || true

# Install systemd service
cat > /etc/systemd/system/nifi.service <<EOF
[Unit]
Description=Apache NiFi
After=network.target

[Service]
Type=forking
User=nifi
Group=nifi
ExecStart=$NIFI_HOME/bin/nifi.sh start
ExecStop=$NIFI_HOME/bin/nifi.sh stop
ExecReload=$NIFI_HOME/bin/nifi.sh restart
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable nifi
systemctl start nifi
