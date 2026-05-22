"""
pytest fixtures for OpenFlow integration tests.

Required environment variables (set via GitHub Secrets or .env file):
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE
  SNOWFLAKE_WAREHOUSE, SNOWFLAKE_SCHEMA
  NIFI_URL, NIFI_ADMIN_PASSWORD
  PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
  AWS_REGION, S3_STAGING_BUCKET
"""

import json
import os
import time
from typing import Generator

import boto3
import psycopg2
import pytest
import requests
import snowflake.connector
import urllib3

urllib3.disable_warnings()


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        pytest.skip(f"Environment variable {name!r} not set — skipping integration test")
    return val


@pytest.fixture(scope="session")
def snowflake_conn():
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    private_key_pem = _require_env("SNOWFLAKE_PRIVATE_KEY").encode()
    private_key = load_pem_private_key(private_key_pem, password=None)

    conn = snowflake.connector.connect(
        account=_require_env("SNOWFLAKE_ACCOUNT"),
        user=_require_env("SNOWFLAKE_USER"),
        private_key=private_key,
        database=_require_env("SNOWFLAKE_DATABASE"),
        warehouse=_require_env("SNOWFLAKE_WAREHOUSE"),
        schema=_require_env("SNOWFLAKE_SCHEMA"),
        role="OPENFLOW_INGEST_ROLE",
        session_parameters={"QUERY_TAG": "openflow-test"},
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(
        host=_require_env("PG_HOST"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=_require_env("PG_DATABASE"),
        user=_require_env("PG_USER"),
        password=_require_env("PG_PASSWORD"),
        sslmode="require",
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def nifi_session():
    import nipyapi

    nifi_url = _require_env("NIFI_URL")
    password = _require_env("NIFI_ADMIN_PASSWORD")
    session = requests.Session()
    session.verify = False

    resp = session.post(
        f"{nifi_url}/nifi-api/access/token",
        data={"username": "admin", "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp.raise_for_status()
    token = resp.text.strip()
    session.headers["Authorization"] = f"Bearer {token}"
    session.base_url = nifi_url

    # Configure nipyapi so schedule_process_group calls work in recovery tests
    nipyapi.config.nifi_config.host = nifi_url.rstrip("/") + "/nifi-api"
    nipyapi.config.nifi_config.verify_ssl = False
    nipyapi.config.nifi_config.api_key = {"tokenAuth": token}
    nipyapi.config.nifi_config.api_key_prefix = {"tokenAuth": "Bearer"}

    return session


@pytest.fixture(scope="session")
def s3_client():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-2"))


@pytest.fixture(scope="session")
def staging_bucket() -> str:
    return _require_env("S3_STAGING_BUCKET")


def wait_for_row_count(
    cursor,
    query: str,
    min_rows: int,
    timeout: int = 120,
    poll_interval: int = 5,
) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        cursor.execute(query)
        count = cursor.fetchone()[0]
        if count >= min_rows:
            return count
        time.sleep(poll_interval)
    cursor.execute(query)
    return cursor.fetchone()[0]
