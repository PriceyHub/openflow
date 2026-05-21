# OpenFlow Runbook

Operational guide for the OpenFlow NiFi ingestion platform.
Account: cngfczx-ow26289 (Snowflake, AWS eu-west-2).

---

## Architecture summary

```
Salesforce REST API
        │
        ▼
 NiFi (QuerySalesforceObject)
        │  JSON records
        ▼
 SplitJson → ConvertRecord → MergeRecord
        │  batched JSON
        ▼
 PutS3Object ──► S3 staging bucket (openflow-staging-{env}-eu-west-2)
        │  success
        ▼
 ExecuteSQL (Snowflake COPY INTO)
        │
        ▼
 Snowflake: {ENV}.RAW.SF_ACCOUNTS_RAW / SF_CONTACTS_RAW


RDS Postgres (WAL / logical replication)
        │
        ▼
 NiFi (CaptureChangePostgreSQL)
        │  CDC events: INSERT / UPDATE / DELETE
        ▼
 RouteOnAttribute → UpdateAttribute → ConvertRecord → MergeRecord
        │  batched JSON
        ▼
 PutS3Object ──► S3 staging bucket
        │  success
        ▼
 ExecuteSQL (COPY INTO RAW) → ExecuteSQL (MERGE INTO target table)
        │
        ▼
 Snowflake: {ENV}.POSTGRES_CDC.CUSTOMERS / ORDERS
```

NiFi Registry (shared, eu-west-2) stores all versioned flow snapshots.
Git (this repo) is the source of truth. Registry is updated on every deploy.

---

## Environments

| Env  | NiFi URL                      | Snowflake DB     | S3 Bucket                            |
|------|-------------------------------|------------------|--------------------------------------|
| dev  | `https://DEV_IP:8443/nifi`    | OPENFLOW_DEV     | openflow-staging-dev-eu-west-2       |
| test | `https://TEST_IP:8443/nifi`   | OPENFLOW_TEST    | openflow-staging-test-eu-west-2      |
| prod | `https://PROD_IP:8443/nifi`   | OPENFLOW_PROD    | openflow-staging-prod-eu-west-2      |

Fill in IPs after Terraform apply.

---

## First-time setup (per environment)

### Prerequisites
- AWS CLI configured with sufficient permissions
- `psql` and `snowsql` in PATH
- Python 3.12+, `pip install -r deploy/requirements.txt`
- Terraform workspace `dev` / `test` / `prod` applied
- Secrets Manager secrets populated (see below)

### 1. Populate AWS Secrets Manager

For each environment, set values via Console or CLI:

```bash
ENV=dev

# Salesforce credentials (from Connected App)
aws secretsmanager put-secret-value \
  --secret-id "openflow/$ENV/salesforce" \
  --secret-string '{"instance_url":"https://yourorg.my.salesforce.com","client_id":"...","client_secret":"..."}'

# Postgres credentials (from RDS console)
aws secretsmanager put-secret-value \
  --secret-id "openflow/$ENV/postgres" \
  --secret-string '{"host":"rds-endpoint.eu-west-2.rds.amazonaws.com","port":"5432","database":"mydb","username":"nifi_cdc","password":"..."}'

# Snowflake credentials
aws secretsmanager put-secret-value \
  --secret-id "openflow/$ENV/snowflake" \
  --secret-string '{"username":"OPENFLOW_NIFI_SVC","password":"..."}'
```

### 2. Terraform

```bash
cd infrastructure/terraform

# First time: create state bucket and DynamoDB lock table manually, then:
terraform init
terraform workspace new dev
terraform apply -var-file environments/dev.tfvars
```

### 3. Bootstrap

```bash
# Wait ~5 minutes for EC2 user-data to finish, then:
./scripts/bootstrap.sh --env dev
```

The bootstrap script:
- Creates Postgres replication slot (`openflow_dev_slot`) and publication
- Runs Snowflake DDL (databases, schemas, tables, roles)
- Creates NiFi Registry bucket `openflow-flows`
- Deploys all flows to dev NiFi

### 4. Snowflake storage integration

After `infrastructure/snowflake/04_roles_grants.sql` runs:

```sql
-- In Snowflake worksheet:
DESC INTEGRATION OPENFLOW_S3_INTEGRATION;
```

Copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.
Update the `aws_iam_role.snowflake_s3_access` trust policy in Terraform or the AWS Console with these values, then re-apply.

---

## Deploying flows

### Via GitHub Actions (normal path)

| Action | Trigger |
|--------|---------|
| Push to `main` | Auto-deploys to Dev, runs tests |
| Promote to Test | `workflow_dispatch` on `promote-test.yml` — requires approval |
| Promote to Prod | `workflow_dispatch` on `promote-prod.yml` — requires 2 approvals, confirmation phrase |

### Manually (breakglass)

```bash
export NIFI_ADMIN_PASSWORD="..."
python deploy/deploy.py --env dev
python deploy/deploy.py --env dev --flow salesforce_ingestion
python deploy/deploy.py --env prod --version 5   # pin to specific registry version
python deploy/deploy.py --env dev --dry-run       # preview only
```

---

## Monitoring flows

### NiFi UI

1. Open `https://{NIFI_IP}:8443/nifi` in a browser.
2. Log in with `admin` / password from Secrets Manager.
3. Process groups `Salesforce Ingestion [env]` and `PostgreSQL CDC [env]` should be green (RUNNING).
4. Bulletin board (top-right bell) shows errors.

### Key metrics to watch

| Metric | Where | Healthy value |
|--------|-------|---------------|
| Queued flowfiles | NiFi canvas / queue labels | 0 (or draining) |
| Processor bulletin errors | NiFi bulletin board | None |
| SF_ACCOUNTS_RAW row count | Snowflake | Growing every ~1 hr |
| POSTGRES_CDC.CUSTOMERS row count | Snowflake | Growing within 60s of PG changes |
| S3 staged/ prefix | S3 console | Files appear and disappear (lifecycle = 3d) |

### Useful Snowflake queries

```sql
-- How many rows landed in the last hour?
SELECT COUNT(*), MIN(_LOADED_AT), MAX(_LOADED_AT)
FROM OPENFLOW_DEV.RAW.SF_ACCOUNTS_RAW
WHERE _LOADED_AT > DATEADD('hour', -1, CURRENT_TIMESTAMP());

-- Latest CDC operations by table
SELECT _CDC_OPERATION, COUNT(*)
FROM OPENFLOW_DEV.POSTGRES_CDC.CUSTOMERS
WHERE _ETL_LOADED_AT > DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY 1;

-- Check for COPY INTO errors in query history
SELECT QUERY_TEXT, ERROR_MESSAGE, START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TYPE = 'COPY'
  AND ERROR_MESSAGE IS NOT NULL
  AND DATABASE_NAME = 'OPENFLOW_DEV'
ORDER BY START_TIME DESC
LIMIT 20;
```

---

## Common operational tasks

### Stop / start a flow

```bash
# Via NiFi UI: select PG → right-click → Stop / Start

# Via API (useful for scripts):
PG_ID="..." # from NiFi UI URL or API
NIFI_URL="https://..."
TOKEN="..."  # Bearer token

curl -s -X PUT "$NIFI_URL/nifi-api/flow/process-groups/$PG_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"'$PG_ID'","state":"STOPPED"}'
```

### Roll back to a previous flow version

```bash
# Find available versions in registry
python deploy/deploy.py --env dev --dry-run
# Check NiFi Registry UI at http://{REGISTRY_IP}:18080

# Deploy specific version
python deploy/deploy.py --env dev --flow salesforce_ingestion --version 3
```

### Re-create a stuck Postgres CDC replication slot

```bash
# Connect to RDS
psql -h $PG_HOST -U $PG_USER -d $PG_DB

-- Check slot lag
SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots;

-- If a slot is stuck (NiFi was stopped for >max_wal_size hours):
SELECT pg_drop_replication_slot('openflow_dev_slot');
SELECT pg_create_logical_replication_slot('openflow_dev_slot', 'pgoutput');
-- Then restart the PostgreSQL CDC process group in NiFi
```

### Clear a back-pressured NiFi queue

1. Stop the upstream processor.
2. Right-click the queue → Empty Queue.
3. Investigate why records backed up (Snowflake connectivity, S3 permissions, etc.).
4. Restart the upstream processor.

---

## Incident response

### No new rows in Snowflake for > 2 hours

1. Check NiFi bulletins — any red processors?
2. Check `QuerySalesforceObject` — Salesforce token may have expired. Re-authorise or rotate client_secret.
3. Check `PutS3Object` — verify NiFi IAM role has `s3:PutObject` on the staging bucket.
4. Check `ExecuteSQL` (COPY INTO) — run the COPY query manually in Snowflake worksheet with `ON_ERROR=ABORT_STATEMENT` to surface errors.
5. Check Snowflake warehouse is not suspended or queued.

### CDC lag growing (Postgres WAL not being consumed)

1. Check `CaptureChangePostgreSQL` processor status in NiFi.
2. Check NiFi → Postgres network connectivity (security group, VPC peering).
3. Check Postgres: `SELECT * FROM pg_replication_slots` — is `active` = true?
4. If NiFi was restarted, the processor may need its state cleared:
   - Right-click processor → View State → Clear State.
   - Restart processor. It will resume from the last committed LSN.
5. If the slot was dropped (WAL gap), re-create it (see above) and accept a re-read from the publication start.

### Snowflake COPY INTO failures

```sql
-- Check recent COPY errors
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME=>'SF_ACCOUNTS_RAW',
  START_TIME=>DATEADD('hour',-2,CURRENT_TIMESTAMP())
))
WHERE STATUS != 'Loaded'
ORDER BY LAST_LOAD_TIME DESC;
```

Common causes:
- JSON parse errors → inspect `_raw_json` sample, fix Salesforce SOQL or ConvertRecord config
- S3 permissions → verify storage integration trust policy
- File not found in stage → S3 lifecycle may have deleted the file before COPY ran; reduce MergeRecord `Max Bin Age`

---

## Secrets rotation

When Salesforce, Postgres, or Snowflake passwords change:

1. Update the secret in AWS Secrets Manager.
2. Re-deploy the affected environment:
   ```bash
   python deploy/deploy.py --env prod --flow salesforce_ingestion
   ```
   The deploy script fetches fresh secrets from Secrets Manager and updates NiFi parameter contexts.
3. NiFi picks up the new values on the next flow schedule (no restart needed for most processors; DBCP connection pools may need their controller service disabled/re-enabled).

---

## GitHub Actions secrets required

| Secret name | Used by | Description |
|-------------|---------|-------------|
| `AWS_DEPLOY_ROLE_ARN` | dev/test | IAM role ARN for OIDC deploy |
| `AWS_PROD_DEPLOY_ROLE_ARN` | prod | Separate role with narrower permissions |
| `NIFI_DEV_URL` | dev tests | `https://IP:8443` |
| `NIFI_TEST_URL` | test | |
| `NIFI_PROD_URL` | prod | |
| `NIFI_DEV_ADMIN_PASSWORD` | dev | NiFi admin password |
| `NIFI_TEST_ADMIN_PASSWORD` | test | |
| `NIFI_PROD_ADMIN_PASSWORD` | prod | |
| `SNOWFLAKE_ACCOUNT` | tests | `cngfczx-ow26289` |
| `SNOWFLAKE_USER` | tests | Service account username |
| `SNOWFLAKE_PASSWORD` | tests | |
| `PG_HOST` | tests | RDS endpoint |
| `PG_DATABASE` | tests | |
| `PG_USER` | tests | |
| `PG_PASSWORD` | tests | |
| `S3_STAGING_BUCKET_DEV` | dev tests | |
| `S3_STAGING_BUCKET_TEST` | test tests | |
| `S3_STAGING_BUCKET_PROD` | prod smoke | |
