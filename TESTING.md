# End-to-End Testing Guide

Two integration patterns are running in the dev environment:
- **Salesforce Ingestion** — hourly batch query → S3 → Snowflake RAW
- **PostgreSQL CDC** — 60-second poll → S3 → Snowflake RAW

---

## Prerequisites

Both SSM tunnels must be open in separate terminals before running any tests or deploys.

**NiFi tunnel (port 8443):**
```bash
aws ssm start-session --target i-0aaf347379a7f6939 \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8443"],"localPortNumber":["8443"]}' \
  --profile terraform-admin --region eu-west-2
```

**NiFi Registry tunnel (port 18080):**
```bash
aws ssm start-session --target i-0b34d96c1a2278245 \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["18080"],"localPortNumber":["18080"]}' \
  --profile terraform-admin --region eu-west-2
```

---

## Test 1 — Smoke Test: Verify Current State

Run in Snowflake (role: `OPENFLOW_INGEST_ROLE`, database: `OPENFLOW_DEV`):

```sql
-- Salesforce
SELECT COUNT(*), MAX(_loaded_at) FROM OPENFLOW_DEV.RAW.SF_ACCOUNTS_RAW;
SELECT COUNT(*), MAX(_loaded_at) FROM OPENFLOW_DEV.RAW.SF_CONTACTS_RAW;

-- PostgreSQL CDC
SELECT COUNT(*), MAX(_loaded_at) FROM OPENFLOW_DEV.RAW.PG_CUSTOMERS_RAW;
SELECT COUNT(*), MAX(_loaded_at) FROM OPENFLOW_DEV.RAW.PG_ORDERS_RAW;
```

**Expected:**
| Table | Rows |
|---|---|
| SF_ACCOUNTS_RAW | ~96,288 |
| SF_CONTACTS_RAW | ~312 |
| PG_CUSTOMERS_RAW | >0, `_loaded_at` within last few minutes |
| PG_ORDERS_RAW | >0, `_loaded_at` within last few minutes |

---

## Test 2 — Live CDC Test: Insert → Snowflake

Confirms a new Postgres record flows through to Snowflake within ~65 seconds.

**Step 1 — Insert a row into Postgres** (via pgAdmin or psql):
```sql
INSERT INTO customers (first_name, last_name, email, phone, status)
VALUES ('Test', 'User', 'test@example.com', '07700000000', 'active');
```
Note the current time.

**Step 2 — Wait 65 seconds.**

**Step 3 — Verify in Snowflake:**
```sql
SELECT _raw_json, _cdc_operation, _loaded_at, _source_file
FROM OPENFLOW_DEV.RAW.PG_CUSTOMERS_RAW
ORDER BY _loaded_at DESC
LIMIT 5;
```

**Expected:** The new row appears with `_CDC_OPERATION = UPSERT` and `_loaded_at` matching the current time. `_source_file` should contain `staged/postgres_cdc/customers/`.

---

## Test 3 — S3 Staging Verification

Confirms files are landing in the correct S3 paths.

```bash
aws s3 ls s3://openflow-staging-dev-eu-west-2/staged/ --recursive \
  --profile terraform-admin --region eu-west-2 | sort | tail -20
```

**Expected paths:**
```
staged/postgres_cdc/customers/YYYY/MM/DD/<uuid>.json
staged/postgres_cdc/orders/YYYY/MM/DD/<uuid>.json
staged/salesforce/accounts/YYYY-MM-DD/<uuid>.json
staged/salesforce/contacts/YYYY-MM-DD/<uuid>.json
```

---

## Test 4 — Full Redeploy (Deploy from Scratch)

The most complete test — validates the entire deploy pipeline including all post-deploy fixups.

```bash
cd deploy
NIFI_ADMIN_PASSWORD=<password> python3 deploy.py --env dev
```

**What this tests:**
- NiFi Registry flow import
- Parameter context upsert (including sensitive params from AWS Secrets Manager)
- ConvertRecord CS reference resolution (`_fix_convert_record_cs_refs`)
- AWSCS patched to `default-credentials=true` (`_fix_aws_credentials_provider`)
- Controller service enable sequence
- Both process groups starting cleanly

**After deploy, run Test 2** to confirm CDC is flowing again (QueryDatabaseTableRecord state is cleared on redeploy, so records will re-flow within 65 seconds).

---

## Test 5 — Salesforce Ingestion Verification

Salesforce runs on a 1-hour schedule. To verify without waiting:

1. Open NiFi canvas at `https://localhost:8443/nifi`
2. Navigate to the **Salesforce Ingestion** process group
3. Right-click **QuerySalesforceObject** → Run Once
4. Wait ~30 seconds for the batch to process through to Snowflake
5. Check `SF_ACCOUNTS_RAW` count has increased

Or verify the last successful run:
```sql
SELECT MAX(_loaded_at) AS last_run,
       COUNT(*) AS total_rows
FROM OPENFLOW_DEV.RAW.SF_ACCOUNTS_RAW;
```

---

## NiFi Canvas

Access at `https://localhost:8443/nifi` (requires NiFi tunnel open).

| Process Group | ID |
|---|---|
| Salesforce Ingestion | `64a22413-019e-1000-2c86-9103b9c93c5c` |
| PostgreSQL CDC | `50b31daf-019e-1000-b637-1ad7173ecce7` |

All processors should show **RUNNING** / **VALID**. Any red warning indicators on processors indicate a bulletin — right-click → View status history to investigate.
