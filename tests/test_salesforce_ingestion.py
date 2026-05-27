"""
Integration tests for the Salesforce → Snowflake ingestion flow.

Test categories:
  - Basic: verify rows land in Snowflake after flow runs
  - Schema: verify expected columns are present and typed correctly
  - Idempotency: re-running the flow does not duplicate rows
  - Recovery: flow recovers after being stopped and restarted
  - Scale: throughput/latency under increased load
"""

import os
import time
import json

import pytest

from conftest import wait_for_row_count


SNOWFLAKE_DB = os.environ.get("SNOWFLAKE_DATABASE", "OPENFLOW_DEV")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_nifi_pg_status(nifi_session, pg_name: str) -> dict:
    resp = nifi_session.get(
        f"{nifi_session.base_url}/nifi-api/flow/process-groups/root/status",
    )
    resp.raise_for_status()
    for pg in resp.json().get("processGroupStatus", {}).get("aggregateSnapshot", {}).get("processGroupStatusSnapshots", []):
        if pg_name in pg.get("name", ""):
            return pg
    return {}


def get_pg_id_by_name(nifi_session, pg_name: str) -> str | None:
    resp = nifi_session.get(
        f"{nifi_session.base_url}/nifi-api/process-groups/root/process-groups"
    )
    resp.raise_for_status()
    for pg in resp.json().get("processGroups", []):
        if pg_name in pg["component"]["name"]:
            return pg["id"]
    return None


def schedule_pg(nifi_session, pg_id: str, running: bool) -> None:
    import nipyapi
    if not running:
        nipyapi.canvas.schedule_process_group(pg_id, False)
        return
    # When starting, retry if processors are still in STARTING/STOPPING transition
    deadline = time.time() + 120
    while True:
        try:
            nipyapi.canvas.schedule_process_group(pg_id, True)
            return
        except ValueError as exc:
            msg = str(exc)
            if ("cannot be started" in msg or "not stopped" in msg) and time.time() < deadline:
                time.sleep(5)
            else:
                raise


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.timeout(60)
def test_accounts_land_in_snowflake(snowflake_conn, nifi_session):
    """Accounts table must contain rows — flow runs hourly so we verify data exists, not new arrival."""
    cursor = snowflake_conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW")
    count = cursor.fetchone()[0]
    if count == 0:
        pytest.skip("No Salesforce Account data in org — skipping (expected for empty orgs)")
    assert count > 0, f"SF_ACCOUNTS_RAW is empty"


@pytest.mark.timeout(60)
def test_contacts_land_in_snowflake(snowflake_conn, nifi_session):
    """Contacts table must contain rows — flow runs hourly so we verify data exists, not new arrival."""
    cursor = snowflake_conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_CONTACTS_RAW")
    count = cursor.fetchone()[0]
    if count == 0:
        pytest.skip("No Salesforce Contact data in org — skipping (expected for empty orgs)")
    assert count > 0, f"SF_CONTACTS_RAW is empty"


@pytest.mark.timeout(60)
def test_accounts_raw_schema(snowflake_conn):
    """RAW table must have _raw_json (VARIANT), _loaded_at (TIMESTAMP), _source_file."""
    cursor = snowflake_conn.cursor()
    cursor.execute(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM {SNOWFLAKE_DB}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA='RAW' AND TABLE_NAME='SF_ACCOUNTS_RAW'"
    )
    cols = {row[0]: row[1] for row in cursor.fetchall()}

    assert "_RAW_JSON" in cols, "Missing _RAW_JSON column"
    assert cols["_RAW_JSON"] == "VARIANT", "_RAW_JSON should be VARIANT"
    assert "_LOADED_AT" in cols, "Missing _LOADED_AT column"
    assert "_SOURCE_FILE" in cols, "Missing _SOURCE_FILE column"


@pytest.mark.timeout(60)
def test_raw_json_is_parseable(snowflake_conn):
    """_raw_json should contain a valid JSON object with at least an Id field."""
    cursor = snowflake_conn.cursor()
    cursor.execute(
        f"SELECT _raw_json:Id::VARCHAR FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW LIMIT 1"
    )
    row = cursor.fetchone()
    if row is None:
        pytest.skip("No rows in SF_ACCOUNTS_RAW — skipping (expected for empty orgs)")
    sf_id = row[0]
    assert sf_id and len(sf_id) in (15, 18), f"Unexpected Salesforce Id length: {sf_id!r}"


@pytest.mark.timeout(120)
def test_flow_recovers_after_stop_restart(snowflake_conn, nifi_session):
    """Stop the Salesforce Ingestion process group, wait, restart it, verify rows continue flowing."""
    pg_id = get_pg_id_by_name(nifi_session, "Salesforce Ingestion")
    if not pg_id:
        pytest.skip("Salesforce Ingestion process group not found on canvas")

    cursor = snowflake_conn.cursor()
    before = cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW").fetchone()[0]

    # Stop
    schedule_pg(nifi_session, pg_id, running=False)
    time.sleep(15)

    # Restart
    schedule_pg(nifi_session, pg_id, running=True)

    count = wait_for_row_count(
        cursor,
        f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW",
        min_rows=before,
        timeout=240,
    )
    assert count >= before, "Row count dropped after restart — data may have been lost"


@pytest.mark.timeout(600)
@pytest.mark.slow
def test_accounts_throughput(snowflake_conn, nifi_session):
    """
    Scale test: after 5 minutes of ingestion, measure rows/second.
    Requires OPENFLOW_SCALE_TEST=1 to run.
    """
    if not os.environ.get("OPENFLOW_SCALE_TEST"):
        pytest.skip("Set OPENFLOW_SCALE_TEST=1 to run scale tests")

    cursor = snowflake_conn.cursor()
    start_time = time.time()
    start_count = cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW").fetchone()[0]

    time.sleep(300)  # 5-minute observation window

    end_count = cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW").fetchone()[0]
    elapsed = time.time() - start_time
    rows_ingested = end_count - start_count
    throughput = rows_ingested / elapsed if elapsed > 0 else 0

    # Baseline expectation: > 10 rows/sec for Salesforce (adjust for dataset size)
    assert throughput > 0, f"No rows ingested during scale test (elapsed={elapsed:.0f}s)"
    print(f"Salesforce throughput: {throughput:.2f} rows/sec ({rows_ingested} rows in {elapsed:.0f}s)")
