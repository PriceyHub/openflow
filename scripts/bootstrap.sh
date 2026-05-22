#!/bin/bash
# bootstrap.sh — first-time setup for a new environment.
# Run this ONCE after Terraform apply to initialise NiFi Registry, create the
# Postgres replication slot/publication, and verify Snowflake connectivity.
#
# Usage: ./scripts/bootstrap.sh --env dev
# Requires: aws cli, psql, snowsql (in PATH), python3 with deploy/requirements.txt

set -euo pipefail

ENV=""
NIFI_REGISTRY_URL=""
DRY_RUN=false

usage() {
  echo "Usage: $0 --env <dev|test|prod> [--registry-url <url>] [--dry-run]"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)          ENV="$2";               shift 2 ;;
    --registry-url) NIFI_REGISTRY_URL="$2"; shift 2 ;;
    --dry-run)      DRY_RUN=true;           shift ;;
    *)              usage ;;
  esac
done

[[ -z "$ENV" ]] && usage

echo "=== OpenFlow Bootstrap: environment=$ENV dry_run=$DRY_RUN ==="

# ─────────────────────────────────────────────
# 1. Resolve config from environment YAML
# ─────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$SCRIPT_DIR/../deploy"
ENV_CONFIG="$DEPLOY_DIR/environments/${ENV}.yml"

[[ ! -f "$ENV_CONFIG" ]] && { echo "ERROR: env config not found: $ENV_CONFIG"; exit 1; }

NIFI_URL=$(python3 -c "import yaml; c=yaml.safe_load(open('$ENV_CONFIG')); print(c['nifi_url'])")
[[ -z "$NIFI_REGISTRY_URL" ]] && NIFI_REGISTRY_URL=$(python3 -c "import yaml; c=yaml.safe_load(open('$ENV_CONFIG')); print(c['nifi_registry_url'])")
AWS_REGION=$(python3 -c "import yaml; c=yaml.safe_load(open('$ENV_CONFIG')); print(c['aws_region'])")
SECRETS_PREFIX=$(python3 -c "import yaml; c=yaml.safe_load(open('$ENV_CONFIG')); print(c['aws_secrets_prefix'])")

echo "NiFi URL:      $NIFI_URL"
echo "Registry URL:  $NIFI_REGISTRY_URL"
echo "AWS Region:    $AWS_REGION"

# ─────────────────────────────────────────────
# 2. Resolve credentials from Secrets Manager
# ─────────────────────────────────────────────
echo ""
echo "--- Resolving Postgres credentials from Secrets Manager ---"
PG_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id "${SECRETS_PREFIX}/postgres" \
  --region "$AWS_REGION" \
  --query SecretString \
  --output text)

PG_HOST=$(echo "$PG_SECRET" | python3 -c "import json,sys; print(json.load(sys.stdin)['host'])")
PG_PORT=$(echo "$PG_SECRET" | python3 -c "import json,sys; print(json.load(sys.stdin).get('port','5432'))")
PG_DB=$(echo "$PG_SECRET"   | python3 -c "import json,sys; print(json.load(sys.stdin)['database'])")
PG_USER=$(echo "$PG_SECRET" | python3 -c "import json,sys; print(json.load(sys.stdin)['username'])")
export PGPASSWORD
PGPASSWORD=$(echo "$PG_SECRET" | python3 -c "import json,sys; print(json.load(sys.stdin)['password'])")

SNOW_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id "${SECRETS_PREFIX}/snowflake" \
  --region "$AWS_REGION" \
  --query SecretString \
  --output text)

SNOW_USER=$(echo "$SNOW_SECRET" | python3 -c "import json,sys; print(json.load(sys.stdin)['username'])")
SNOW_PASS=$(echo "$SNOW_SECRET" | python3 -c "import json,sys; print(json.load(sys.stdin)['password'])")

# ─────────────────────────────────────────────
# 3. PostgreSQL: create replication slot & publication
# ─────────────────────────────────────────────
echo ""
echo "--- PostgreSQL: creating replication slot and publication ---"
SLOT_NAME="openflow_${ENV}_slot"
PUBLICATION="openflow_pub"

if $DRY_RUN; then
  echo "[DRY RUN] Would create slot '$SLOT_NAME' and publication '$PUBLICATION' on $PG_HOST:$PG_PORT/$PG_DB"
else
  psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" <<SQL
-- Idempotent: only create if not existing
SELECT CASE WHEN NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'
) THEN pg_create_logical_replication_slot('$SLOT_NAME', 'pgoutput')
END;

DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '$PUBLICATION') THEN
    CREATE PUBLICATION $PUBLICATION FOR TABLE customers, orders;
    RAISE NOTICE 'Created publication $PUBLICATION';
  ELSE
    RAISE NOTICE 'Publication $PUBLICATION already exists';
  END IF;
END;
\$\$;
SQL
  echo "Postgres slot '$SLOT_NAME' and publication '$PUBLICATION' ready."
fi

# ─────────────────────────────────────────────
# 4. Snowflake: run DDL scripts
# ─────────────────────────────────────────────
echo ""
echo "--- Snowflake: applying DDL ---"
SNOW_ENV_UPPER="${ENV^^}"
SNOWFLAKE_ACCOUNT="cngfczx-ow26289"

if $DRY_RUN; then
  echo "[DRY RUN] Would run Snowflake DDL scripts against OPENFLOW_${SNOW_ENV_UPPER}"
else
  for sql_file in "$SCRIPT_DIR/../infrastructure/snowflake/"*.sql; do
    echo "Running $sql_file..."
    snowsql \
      -a "$SNOWFLAKE_ACCOUNT" \
      -u "$SNOW_USER" \
      --password "$SNOW_PASS" \
      -v db_name="OPENFLOW_${SNOW_ENV_UPPER}" \
      -v wh_name="OPENFLOW_INGEST_WH_${SNOW_ENV_UPPER}" \
      -f "$sql_file" \
      --noup 2>&1 | tail -5 || echo "WARNING: $sql_file returned non-zero (check manually)"
  done
  echo "Snowflake DDL complete."
fi

# ─────────────────────────────────────────────
# 5. NiFi Registry: create bucket
# ─────────────────────────────────────────────
echo ""
echo "--- NiFi Registry: ensuring bucket exists ---"
if $DRY_RUN; then
  echo "[DRY RUN] Would create bucket 'openflow-flows' in Registry at $NIFI_REGISTRY_URL"
else
  BUCKET_CHECK=$(curl -s "${NIFI_REGISTRY_URL}/nifi-registry-api/buckets" | python3 -c "
import json, sys
buckets = json.load(sys.stdin)
names = [b.get('name','') for b in buckets]
print('exists' if 'openflow-flows' in names else 'missing')
" 2>/dev/null || echo "error")

  if [[ "$BUCKET_CHECK" == "missing" ]]; then
    curl -s -X POST "${NIFI_REGISTRY_URL}/nifi-registry-api/buckets" \
      -H "Content-Type: application/json" \
      -d '{"name":"openflow-flows","description":"OpenFlow versioned NiFi flows"}' | python3 -m json.tool
    echo "Created bucket: openflow-flows"
  else
    echo "Bucket 'openflow-flows' already exists or Registry not reachable (${BUCKET_CHECK})"
  fi
fi

# ─────────────────────────────────────────────
# 6. Initial flow deployment
# ─────────────────────────────────────────────
echo ""
echo "--- Deploying flows ---"
if $DRY_RUN; then
  python3 "$DEPLOY_DIR/deploy.py" --env "$ENV" --dry-run
else
  NIFI_ADMIN_PASSWORD="$(aws secretsmanager get-secret-value \
    --secret-id "${SECRETS_PREFIX}/nifi-admin" \
    --region "$AWS_REGION" \
    --query SecretString \
    --output text 2>/dev/null || echo "${NIFI_ADMIN_PASSWORD:-}")"
  export NIFI_ADMIN_PASSWORD
  python3 "$DEPLOY_DIR/deploy.py" --env "$ENV"
fi

echo ""
echo "=== Bootstrap complete for environment: $ENV ==="
