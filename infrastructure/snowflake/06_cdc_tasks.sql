-- Snowflake Tasks: COPY staged S3 files into RAW then MERGE into typed tables.
-- Run as SYSADMIN via bootstrap.sh (which passes db_name and wh_name).
--
-- Manual usage:
--   snowsql -a cngfczx-ow26289 -u <admin_user> \
--     -v db_name=OPENFLOW_DEV -v wh_name=OPENFLOW_INGEST_WH_DEV \
--     -f 06_cdc_tasks.sql

USE ROLE SYSADMIN;
USE DATABASE IDENTIFIER($db_name);

GRANT CREATE TASK ON SCHEMA POSTGRES_CDC TO ROLE OPENFLOW_INGEST_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE OPENFLOW_INGEST_ROLE;

USE ROLE OPENFLOW_INGEST_ROLE;
USE SCHEMA POSTGRES_CDC;

-- ============================================================
-- Task 1: COPY staged S3 files into RAW tables every minute
-- ============================================================

CREATE OR REPLACE TASK CDC_COPY_INTO_RAW_TASK
  WAREHOUSE = $wh_name
  SCHEDULE  = '1 MINUTE'
  COMMENT   = 'Load new CDC JSON files from S3 into RAW tables'
AS
BEGIN
  COPY INTO RAW.PG_CUSTOMERS_RAW
    (_raw_json, _cdc_operation, _cdc_timestamp, _source_file, _batch_id)
  FROM (
    SELECT $1, 'UPSERT', CURRENT_TIMESTAMP(), METADATA$FILENAME, METADATA$FILENAME
    FROM @RAW.OPENFLOW_S3_STAGE/postgres_cdc/customers/
  )
  FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)
  ON_ERROR   = 'CONTINUE'
  PURGE      = FALSE;

  COPY INTO RAW.PG_ORDERS_RAW
    (_raw_json, _cdc_operation, _cdc_timestamp, _source_file, _batch_id)
  FROM (
    SELECT $1, 'UPSERT', CURRENT_TIMESTAMP(), METADATA$FILENAME, METADATA$FILENAME
    FROM @RAW.OPENFLOW_S3_STAGE/postgres_cdc/orders/
  )
  FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)
  ON_ERROR   = 'CONTINUE'
  PURGE      = FALSE;
END;

-- ============================================================
-- Task 2: MERGE RAW into typed tables — chained after Task 1
-- ============================================================

CREATE OR REPLACE TASK CDC_MERGE_INTO_TYPED_TASK
  WAREHOUSE = $wh_name
  AFTER     CDC_COPY_INTO_RAW_TASK
  COMMENT   = 'Merge RAW CDC rows into typed POSTGRES_CDC tables'
AS
BEGIN
  MERGE INTO CUSTOMERS AS tgt
  USING (
    SELECT
      _raw_json:id::INTEGER               AS ID,
      _raw_json:first_name::VARCHAR(100)  AS FIRST_NAME,
      _raw_json:last_name::VARCHAR(100)   AS LAST_NAME,
      _raw_json:email::VARCHAR(255)       AS EMAIL,
      _raw_json:phone::VARCHAR(50)        AS PHONE,
      _raw_json:status::VARCHAR(20)       AS STATUS,
      _raw_json:created_at::TIMESTAMP_NTZ AS CREATED_AT,
      _raw_json:updated_at::TIMESTAMP_NTZ AS UPDATED_AT,
      _cdc_operation, _cdc_timestamp, _batch_id
    FROM RAW.PG_CUSTOMERS_RAW
  ) AS src ON tgt.ID = src.ID
  WHEN MATCHED THEN UPDATE SET
    tgt.FIRST_NAME     = src.FIRST_NAME,
    tgt.LAST_NAME      = src.LAST_NAME,
    tgt.EMAIL          = src.EMAIL,
    tgt.PHONE          = src.PHONE,
    tgt.STATUS         = src.STATUS,
    tgt.UPDATED_AT     = src.UPDATED_AT,
    tgt._CDC_OPERATION = src._CDC_OPERATION,
    tgt._CDC_TIMESTAMP = src._CDC_TIMESTAMP,
    tgt._ETL_LOADED_AT = CURRENT_TIMESTAMP(),
    tgt._ETL_BATCH_ID  = src._BATCH_ID
  WHEN NOT MATCHED THEN INSERT
    (ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, STATUS, CREATED_AT, UPDATED_AT,
     _CDC_OPERATION, _CDC_TIMESTAMP, _ETL_LOADED_AT, _ETL_BATCH_ID)
  VALUES
    (src.ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.PHONE, src.STATUS,
     src.CREATED_AT, src.UPDATED_AT, src._CDC_OPERATION, src._CDC_TIMESTAMP,
     CURRENT_TIMESTAMP(), src._BATCH_ID);

  MERGE INTO ORDERS AS tgt
  USING (
    SELECT
      _raw_json:id::INTEGER                 AS ID,
      _raw_json:customer_id::INTEGER        AS CUSTOMER_ID,
      _raw_json:order_date::DATE            AS ORDER_DATE,
      _raw_json:status::VARCHAR(30)         AS STATUS,
      _raw_json:total_amount::NUMBER(12,2)  AS TOTAL_AMOUNT,
      _raw_json:currency::VARCHAR(3)        AS CURRENCY,
      _raw_json:created_at::TIMESTAMP_NTZ   AS CREATED_AT,
      _raw_json:updated_at::TIMESTAMP_NTZ   AS UPDATED_AT,
      _cdc_operation, _cdc_timestamp, _batch_id
    FROM RAW.PG_ORDERS_RAW
  ) AS src ON tgt.ID = src.ID
  WHEN MATCHED THEN UPDATE SET
    tgt.CUSTOMER_ID    = src.CUSTOMER_ID,
    tgt.STATUS         = src.STATUS,
    tgt.TOTAL_AMOUNT   = src.TOTAL_AMOUNT,
    tgt.UPDATED_AT     = src.UPDATED_AT,
    tgt._CDC_OPERATION = src._CDC_OPERATION,
    tgt._CDC_TIMESTAMP = src._CDC_TIMESTAMP,
    tgt._ETL_LOADED_AT = CURRENT_TIMESTAMP(),
    tgt._ETL_BATCH_ID  = src._BATCH_ID
  WHEN NOT MATCHED THEN INSERT
    (ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, CURRENCY,
     CREATED_AT, UPDATED_AT, _CDC_OPERATION, _CDC_TIMESTAMP, _ETL_LOADED_AT, _ETL_BATCH_ID)
  VALUES
    (src.ID, src.CUSTOMER_ID, src.ORDER_DATE, src.STATUS, src.TOTAL_AMOUNT, src.CURRENCY,
     src.CREATED_AT, src.UPDATED_AT, src._CDC_OPERATION, src._CDC_TIMESTAMP,
     CURRENT_TIMESTAMP(), src._BATCH_ID);
END;

-- Activate: dependent task first, then root task to start the chain
ALTER TASK CDC_MERGE_INTO_TYPED_TASK RESUME;
ALTER TASK CDC_COPY_INTO_RAW_TASK    RESUME;
