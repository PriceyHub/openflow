"""Shared helpers for NiFi deployment scripts."""

import json
import logging
import time
from pathlib import Path
from typing import Any

import boto3
import nipyapi
import requests
import yaml

logger = logging.getLogger(__name__)


def load_env_config(env: str) -> dict:
    config_path = Path(__file__).parent / "environments" / f"{env}.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def load_flow_json(flow_file: str) -> dict:
    path = Path(__file__).parent / flow_file
    with open(path) as f:
        return json.load(f)


def load_parameter_context_file(param_file: str) -> dict:
    path = Path(__file__).parent / param_file
    with open(path) as f:
        return json.load(f)


def configure_nipyapi(nifi_url: str, registry_url: str, username: str, password: str) -> None:
    nipyapi.config.nifi_config.host = f"{nifi_url}/nifi-api"
    nipyapi.config.nifi_config.verify_ssl = False

    nipyapi.config.registry_config.host = f"{registry_url}/nifi-registry-api"
    nipyapi.config.registry_config.verify_ssl = False

    nipyapi.security.service_login(
        service="nifi",
        username=username,
        password=password,
    )

    # Suppress SSL warnings during dev/test
    requests.packages.urllib3.disable_warnings()
    logger.info("nipyapi configured — NiFi: %s | Registry: %s", nifi_url, registry_url)


def wait_for_nifi(nifi_url: str, timeout: int = 600) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(
                f"{nifi_url}/nifi-api/system-diagnostics",
                verify=False,
                timeout=5,
            )
            # 200 = up + authed, 401 = up but requires auth — both mean NiFi is ready
            if resp.status_code in (200, 401):
                logger.info("NiFi is ready at %s (HTTP %d)", nifi_url, resp.status_code)
                return
        except requests.exceptions.ConnectionError:
            pass
        logger.debug("Waiting for NiFi...")
        time.sleep(5)
    raise TimeoutError(f"NiFi did not become ready within {timeout}s at {nifi_url}")


def resolve_secrets_from_aws(param_contexts: dict, secrets_prefix: str, region: str, profile: str = None) -> dict:
    """Replace REPLACE_FROM_AWS_SECRETS placeholders with real values from Secrets Manager."""
    import botocore.session as _bc
    effective_profile = profile if profile and profile in _bc.Session().available_profiles else None
    session = boto3.Session(profile_name=effective_profile) if effective_profile else boto3.Session()
    sm = session.client("secretsmanager", region_name=region)
    resolved = {}

    secret_map = {}
    for suffix in ("salesforce", "postgres", "snowflake"):
        try:
            resp = sm.get_secret_value(SecretId=f"{secrets_prefix}/{suffix}")
            secret_map[suffix] = json.loads(resp["SecretString"])
            logger.info("Loaded secret: %s/%s", secrets_prefix, suffix)
        except Exception as exc:
            logger.warning("Could not load secret %s/%s: %s", secrets_prefix, suffix, exc)

    sf_secret = secret_map.get("salesforce", {})
    pg_secret = secret_map.get("postgres", {})
    snow_secret = secret_map.get("snowflake", {})

    for ctx_name, params in param_contexts.items():
        resolved[ctx_name] = dict(params)
        r = resolved[ctx_name]
        if "salesforce_client_id" in r and sf_secret:
            r["salesforce_instance_url"] = sf_secret.get("instance_url", r["salesforce_instance_url"])
            r["salesforce_client_id"] = sf_secret.get("client_id", r["salesforce_client_id"])
            r["salesforce_client_secret"] = sf_secret.get("client_secret", r["salesforce_client_secret"])
        if "postgres_password" in r and pg_secret:
            r["postgres_host"] = pg_secret.get("host", r["postgres_host"])
            r["postgres_database"] = pg_secret.get("database", r["postgres_database"])
            r["postgres_username"] = pg_secret.get("username", r["postgres_username"])
            r["postgres_password"] = pg_secret.get("password", r["postgres_password"])
        if "snowflake_password" in r and snow_secret:
            r["snowflake_username"] = snow_secret.get("username", r["snowflake_username"])
            r["snowflake_password"] = snow_secret.get("password", r["snowflake_password"])

    return resolved


def get_or_create_registry_bucket(bucket_name: str) -> Any:
    buckets = nipyapi.versioning.list_registry_buckets()
    if buckets:
        for b in buckets:
            if b.name == bucket_name:
                logger.info("Registry bucket exists: %s", bucket_name)
                return b
    bucket = nipyapi.versioning.create_registry_bucket(bucket_name)
    logger.info("Created registry bucket: %s", bucket_name)
    return bucket


def get_root_pg_id() -> str:
    return nipyapi.canvas.get_root_pg_id()
