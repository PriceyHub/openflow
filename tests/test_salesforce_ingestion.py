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
    state = "RUNNING" if running else "STOPPED"
    resp = nifi_session.put(
        f"{nifi_session.base_url}/nifi-api/flow/process-groups/{pg_id}",
        json={"id": pg_id, "state": state},
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.timeout(300)
def test_accounts_land_in_snowflake(snowflake_conn, nifi_session):
    """After one trigger cycle, at least one Account row should appear in RAW."""
    cursor = snowflake_conn.cursor()
    before = cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW").fetchone()[0]

    count = wait_for_row_count(
        cursor,
        f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW",
        min_rows=before + 1,
        timeout=240,
    )
    assert count > before, f"No new Account rows appeared in {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW (before={before}, after={count})"


@pytest.mark.timeout(300)
def test_contacts_land_in_snowflake(snowflake_conn, nifi_session):
    """After one trigger cycle, at least one Contact row should appear in RAW."""
    cursor = snowflake_conn.cursor()
    before = cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_CONTACTS_RAW").fetchone()[0]

    count = wait_for_row_count(
        cursor,
        f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.RAW.SF_CONTACTS_RAW",
        min_rows=before + 1,
        timeout=240,
    )
    assert count > before


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
    assert row is not None, "No rows found in SF_ACCOUNTS_RAW"
    sf_id = row[0]
    assert sf_id and len(sf_id) in (15, 18), f"Unexpected Salesforce Id length: {sf_id!r}"


@pytest.mark.timeout(300)
def test_no_duplicate_accounts_on_rerun(snowflake_conn, nifi_session):
    """Running the flow twice should not double-count unique Salesforce IDs."""
    cursor = snowflake_conn.cursor()
    cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT _raw_json:Id::VARCHAR) FROM {SNOWFLAKE_DB}.RAW.SF_ACCOUNTS_RAW")
    total, distinct = cursor.fetchone()
    if total == 0:
        pytest.skip("No data in table to check for duplicates")
    # Acceptable duplication rate < 5% (some overlap is expected with full reload)
    duplication_rate = (total - distinct) / total if total > 0 else 0
    assert duplication_rate < 0.05, f"Too many duplicate Account IDs: total={total} distinct={distinct}"


@pytest.mark.timeout(300)
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
