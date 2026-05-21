# OpenFlow

NiFi-based data ingestion platform on AWS eu-west-2.

| Pattern | Source | Sink |
|---------|--------|------|
| Batch | Salesforce (Account, Contact) | Snowflake `cngfczx-ow26289` |
| CDC | RDS Postgres (customers, orders) | Snowflake `cngfczx-ow26289` |

## Repository layout

```
.github/workflows/      CI/CD — deploy-dev, promote-test, promote-prod
deploy/                 Python/nipyapi deployment scripts + env configs
  environments/         dev.yml, test.yml, prod.yml
infrastructure/
  terraform/            AWS infra (NiFi EC2, NiFi Registry, S3, IAM, Secrets)
  snowflake/            DDL: databases, schemas, tables, roles
nifi/
  flows/                Versioned flow JSON (salesforce_ingestion, postgres_cdc)
  parameter_contexts/   Environment-specific parameter values
scripts/                bootstrap.sh — first-time per-env setup
tests/                  pytest integration tests (Salesforce + CDC)
runbook.md              Day-2 operations guide
```

## Quick start

### 1. Infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform workspace new dev
terraform apply -var-file environments/dev.tfvars
```

### 2. Bootstrap (once per env)

```bash
# Populate secrets first — see runbook.md
./scripts/bootstrap.sh --env dev
```

### 3. Deploy flows manually

```bash
pip install -r deploy/requirements.txt
export NIFI_ADMIN_PASSWORD="..."
python deploy/deploy.py --env dev
```

### 4. CI/CD

| Event | Action |
|-------|--------|
| Push to `main` | Auto-deploy to dev + run tests |
| `promote-test.yml` dispatch | Manual approval → test deploy + tests |
| `promote-prod.yml` dispatch | 2 approvals + confirmation phrase → prod |

## Environment variables / secrets

All sensitive values live in **AWS Secrets Manager** under `openflow/{env}/salesforce`, `.../postgres`, `.../snowflake`.
The deploy script fetches them at runtime — no secrets in this repository.

See [runbook.md](runbook.md) for full operational documentation.
