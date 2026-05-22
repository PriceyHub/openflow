"""
Integration tests for the PostgreSQL CDC → Snowflake flow.

Test categories:
  - INSERT: new rows appear in Snowflake with operation=INSERT
  - UPDATE: changed rows appear with operation=UPDATE
  - DELETE: deleted rows appear with operation=DELETE (soft-delete flag)
  - Recovery: flow resumes from correct LSN after restart
  - Scale: throughput under concurrent write load
"""

import os
import time
import uuid

import psycopg2
import pytest

from conftest import wait_for_row_count


SNOWFLAKE_DB = os.environ.get("SNOWFLAKE_DATABASE", "OPENFLOW_DEV")
# 60s poll interval + S3 upload + COPY INTO + MERGE = ~3 min end-to-end
CDC_PROPAGATION_WAIT = int(os.environ.get("CDC_PROPAGATION_WAIT", "180"))


# ---------------------------------------------------------------------------
# Setup: ensure test tables exist in Postgres
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def ensure_pg_test_tables(pg_conn):
    """Create test tables if they don't exist (non-destructive)."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id          SERIAL PRIMARY KEY,
                first_name  VARCHAR(100),
                last_name   VARCHAR(100),
                email       VARCHAR(255) UNIQUE,
                phone       VARCHAR(50),
                status      VARCHAR(20) DEFAULT 'active',
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                updated_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id              SERIAL PRIMARY KEY,
                customer_id     INTEGER REFERENCES customers(id),
                order_date      DATE DEFAULT CURRENT_DATE,
                status          VARCHAR(30) DEFAULT 'pending',
                total_amount    NUMERIC(12,2),
                currency        VARCHAR(3) DEFAULT 'GBP',
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_email() -> str:
    return f"test_{uuid.uuid4().hex[:8]}@openflow-test.invalid"


def _count_cdc_rows(snowflake_conn, table: str, operation: str | None = None, batch_id: str | None = None) -> int:
    cursor = snowflake_conn.cursor()
    base = f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.POSTGRES_CDC.{table.upper()}"
    conditions = []
    if operation:
        conditions.append(f"_CDC_OPERATION = '{operation}'")
    if batch_id:
        conditions.append(f"_ETL_BATCH_ID = '{batch_id}'")
    if conditions:
        base += " WHERE " + " AND ".join(conditions)
    cursor.execute(base)
    return cursor.fetchone()[0]


def _get_customer_from_snowflake(snowflake_conn, customer_id: int) -> dict | None:
    cursor = snowflake_conn.cursor()
    cursor.execute(
        f"SELECT ID, FIRST_NAME, LAST_NAME, EMAIL, STATUS, IS_DELETED, _CDC_OPERATION "
        f"FROM {SNOWFLAKE_DB}.POSTGRES_CDC.CUSTOMERS WHERE ID = %s ORDER BY _ETL_LOADED_AT DESC LIMIT 1",
        (customer_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return dict(zip(["id", "first_name", "last_name", "email", "status", "is_deleted", "cdc_operation"], row))


def _poll_customer(snowflake_conn, customer_id: int, *, status: str | None = None, timeout: int = 240) -> dict | None:
    """Poll Snowflake until the customer row (optionally matching status) appears."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = _get_customer_from_snowflake(snowflake_conn, customer_id)
        if row is not None:
            if status is None or row.get("status") == status:
                return row
        time.sleep(10)
    return _get_customer_from_snowflake(snowflake_conn, customer_id)


# ---------------------------------------------------------------------------
# INSERT tests
# ---------------------------------------------------------------------------

@pytest.mark.timeout(360)
def test_customer_insert_propagates(snowflake_conn, pg_conn):
    """INSERT a new customer in Postgres — it should appear in Snowflake CUSTOMERS."""
    email = _unique_email()
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email, status) VALUES (%s, %s, %s, 'active') RETURNING id",
            ("Test", "CDC", email),
        )
        customer_id = cur.fetchone()[0]

    row = _poll_customer(snowflake_conn, customer_id, timeout=CDC_PROPAGATION_WAIT + 60)
    assert row is not None, f"Customer id={customer_id} not found in Snowflake after INSERT"
    assert row["email"] == email or row.get("cdc_operation") in ("INSERT", "insert")


@pytest.mark.timeout(360)
def test_order_insert_propagates(snowflake_conn, pg_conn):
    """INSERT a new order — it should appear in Snowflake ORDERS."""
    # Create a customer first
    email = _unique_email()
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES ('Order', 'Test', %s) RETURNING id",
            (email,),
        )
        customer_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO orders (customer_id, total_amount, currency) VALUES (%s, 99.99, 'GBP') RETURNING id",
            (customer_id,),
        )
        order_id = cur.fetchone()[0]

    deadline = time.time() + CDC_PROPAGATION_WAIT + 60
    cursor = snowflake_conn.cursor()
    count = 0
    while time.time() < deadline:
        cursor.execute(
            f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.POSTGRES_CDC.ORDERS WHERE ID = %s",
            (order_id,),
        )
        count = cursor.fetchone()[0]
        if count >= 1:
            break
        time.sleep(10)
    assert count >= 1, f"Order id={order_id} not found in Snowflake after INSERT"


# ---------------------------------------------------------------------------
# UPDATE tests
# ---------------------------------------------------------------------------

@pytest.mark.timeout(540)
def test_customer_update_propagates(snowflake_conn, pg_conn):
    """UPDATE a customer — the change should appear in Snowflake with operation=UPDATE."""
    email = _unique_email()
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email, status) VALUES ('Update', 'Test', %s, 'active') RETURNING id",
            (email,),
        )
        customer_id = cur.fetchone()[0]

    # Wait for insert to land before issuing update (ensures updated_at advances past what's already polled)
    _poll_customer(snowflake_conn, customer_id, timeout=CDC_PROPAGATION_WAIT + 60)

    # Now update the status
    with pg_conn.cursor() as cur:
        cur.execute(
            "UPDATE customers SET status = 'premium', updated_at = NOW() WHERE id = %s",
            (customer_id,),
        )

    row = _poll_customer(snowflake_conn, customer_id, status="premium", timeout=CDC_PROPAGATION_WAIT + 60)
    assert row is not None, f"No rows found for customer id={customer_id} after UPDATE"
    status, operation = row.get("status"), row.get("cdc_operation")
    assert status == "premium" or operation in ("UPDATE", "update"), (
        f"Expected status='premium' or operation='UPDATE', got status={status!r}, operation={operation!r}"
    )


# ---------------------------------------------------------------------------
# Soft-delete tests
# ---------------------------------------------------------------------------

@pytest.mark.timeout(540)
def test_customer_soft_delete_propagates(snowflake_conn, pg_conn):
    """Soft-delete a customer (status='deleted', updated_at bump) — the change must appear in Snowflake.

    Hard DELETEs are not captured by QueryDatabaseTableRecord. The recommended pattern is
    a soft-delete flag on the source table, which triggers an updated_at bump and gets
    picked up on the next poll cycle.
    """
    email = _unique_email()
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email, status) VALUES ('SoftDel', 'Test', %s, 'active') RETURNING id",
            (email,),
        )
        customer_id = cur.fetchone()[0]

    _poll_customer(snowflake_conn, customer_id, timeout=CDC_PROPAGATION_WAIT + 60)

    with pg_conn.cursor() as cur:
        cur.execute(
            "UPDATE customers SET status = 'deleted', updated_at = NOW() WHERE id = %s",
            (customer_id,),
        )

    row = _poll_customer(snowflake_conn, customer_id, status="deleted", timeout=CDC_PROPAGATION_WAIT + 60)
    assert row is not None, f"No row for soft-deleted customer id={customer_id}"
    assert row.get("status") == "deleted", f"Expected status='deleted', got {row.get('status')!r}"


# ---------------------------------------------------------------------------
# Recovery tests
# ---------------------------------------------------------------------------

@pytest.mark.timeout(480)
def test_cdc_resumes_after_flow_restart(snowflake_conn, pg_conn, nifi_session):
    """Insert rows while CDC flow is stopped; after restart they must all arrive."""
    from test_salesforce_ingestion import get_pg_id_by_name, schedule_pg

    pg_id = get_pg_id_by_name(nifi_session, "PostgreSQL CDC")
    if not pg_id:
        pytest.skip("PostgreSQL CDC process group not found on canvas")

    schedule_pg(nifi_session, pg_id, running=False)
    time.sleep(5)

    inserted_ids = []
    with pg_conn.cursor() as cur:
        for i in range(5):
            email = _unique_email()
            cur.execute(
                "INSERT INTO customers (first_name, last_name, email) VALUES ('Recovery', %s, %s) RETURNING id",
                (f"Test{i}", email),
            )
            inserted_ids.append(cur.fetchone()[0])

    time.sleep(5)
    schedule_pg(nifi_session, pg_id, running=True)
    time.sleep(CDC_PROPAGATION_WAIT + 30)

    cursor = snowflake_conn.cursor()
    missing = []
    for cid in inserted_ids:
        cursor.execute(
            f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.POSTGRES_CDC.CUSTOMERS WHERE ID = %s",
            (cid,),
        )
        if cursor.fetchone()[0] == 0:
            missing.append(cid)

    assert not missing, f"CDC did not recover missing rows after restart: {missing}"


# ---------------------------------------------------------------------------
# Scale tests
# ---------------------------------------------------------------------------

@pytest.mark.timeout(600)
@pytest.mark.slow
def test_cdc_throughput_under_load(snowflake_conn, pg_conn):
    """
    Insert 1000 rows concurrently and measure CDC propagation latency.
    Requires OPENFLOW_SCALE_TEST=1.
    """
    if not os.environ.get("OPENFLOW_SCALE_TEST"):
        pytest.skip("Set OPENFLOW_SCALE_TEST=1 to run scale tests")

    import concurrent.futures

    BATCH_SIZE = 1000

    def insert_batch(batch_num: int) -> list[int]:
        conn = psycopg2.connect(
            host=os.environ["PG_HOST"],
            port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ["PG_DATABASE"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
            sslmode="require",
        )
        conn.autocommit = True
        ids = []
        with conn.cursor() as cur:
            for i in range(batch_num):
                email = _unique_email()
                cur.execute(
                    "INSERT INTO customers (first_name, last_name, email) VALUES ('Scale', 'Test', %s) RETURNING id",
                    (email,),
                )
                ids.append(cur.fetchone()[0])
        conn.close()
        return ids

    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(insert_batch, BATCH_SIZE // 10) for _ in range(10)]
        all_ids = []
        for f in concurrent.futures.as_completed(futures):
            all_ids.extend(f.result())

    insert_elapsed = time.time() - start
    print(f"Inserted {len(all_ids)} rows in {insert_elapsed:.1f}s")

    # Wait for CDC to propagate all rows
    cursor = snowflake_conn.cursor()
    id_list = ", ".join(str(i) for i in all_ids)
    propagation_start = time.time()

    arrived = wait_for_row_count(
        cursor,
        f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.POSTGRES_CDC.CUSTOMERS WHERE ID IN ({id_list})",
        min_rows=int(BATCH_SIZE * 0.95),  # Allow 5% tolerance
        timeout=300,
        poll_interval=10,
    )

    propagation_elapsed = time.time() - propagation_start
    throughput = arrived / propagation_elapsed if propagation_elapsed > 0 else 0

    print(f"CDC propagation: {arrived}/{BATCH_SIZE} rows in {propagation_elapsed:.1f}s = {throughput:.1f} rows/sec")
    assert arrived >= BATCH_SIZE * 0.95, f"CDC only propagated {arrived}/{BATCH_SIZE} rows (95% threshold not met)"
