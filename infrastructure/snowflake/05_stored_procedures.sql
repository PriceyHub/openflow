-- Stored procedures for CDC merge operations.
-- Run against OPENFLOW_DEV, OPENFLOW_TEST, OPENFLOW_PROD.
--   snowsql -v db_name=OPENFLOW_DEV -f 05_stored_procedures.sql

USE ROLE SYSADMIN;
USE DATABASE IDENTIFIER($db_name);
USE SCHEMA POSTGRES_CDC;

-- Merges rows from the RAW staging table into the typed CDC target table.
-- Called by NiFi ExecuteSQL after each COPY INTO batch.
-- Idempotent: MERGE ON primary key means re-runs are safe.
CREATE OR REPLACE PROCEDURE POSTGRES_CDC.MERGE_CDC_BATCH(TABLE_NAME VARCHAR, BATCH_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    CASE TABLE_NAME
        WHEN 'customers' THEN
            MERGE INTO POSTGRES_CDC.CUSTOMERS AS tgt
            USING (
                SELECT
                    _raw_json:id::INTEGER                AS ID,
                    _raw_json:first_name::VARCHAR(100)   AS FIRST_NAME,
                    _raw_json:last_name::VARCHAR(100)    AS LAST_NAME,
                    _raw_json:email::VARCHAR(255)        AS EMAIL,
                    _raw_json:phone::VARCHAR(50)         AS PHONE,
                    _raw_json:status::VARCHAR(20)        AS STATUS,
                    _raw_json:created_at::TIMESTAMP_NTZ  AS CREATED_AT,
                    _raw_json:updated_at::TIMESTAMP_NTZ  AS UPDATED_AT,
                    _cdc_operation,
                    _cdc_timestamp,
                    _batch_id
                FROM RAW.PG_CUSTOMERS_RAW
            ) AS src ON tgt.ID = src.ID
            WHEN MATCHED THEN UPDATE SET
                tgt.FIRST_NAME      = src.FIRST_NAME,
                tgt.LAST_NAME       = src.LAST_NAME,
                tgt.EMAIL           = src.EMAIL,
                tgt.PHONE           = src.PHONE,
                tgt.STATUS          = src.STATUS,
                tgt.UPDATED_AT      = src.UPDATED_AT,
                tgt._CDC_OPERATION  = src._CDC_OPERATION,
                tgt._CDC_TIMESTAMP  = src._CDC_TIMESTAMP,
                tgt._ETL_LOADED_AT  = CURRENT_TIMESTAMP(),
                tgt._ETL_BATCH_ID   = src._BATCH_ID
            WHEN NOT MATCHED THEN INSERT (
                ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, STATUS,
                CREATED_AT, UPDATED_AT, _CDC_OPERATION, _CDC_TIMESTAMP,
                _ETL_LOADED_AT, _ETL_BATCH_ID
            ) VALUES (
                src.ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.PHONE, src.STATUS,
                src.CREATED_AT, src.UPDATED_AT, src._CDC_OPERATION, src._CDC_TIMESTAMP,
                CURRENT_TIMESTAMP(), src._BATCH_ID
            );

        WHEN 'orders' THEN
            MERGE INTO POSTGRES_CDC.ORDERS AS tgt
            USING (
                SELECT
                    _raw_json:id::INTEGER                AS ID,
                    _raw_json:customer_id::INTEGER       AS CUSTOMER_ID,
                    _raw_json:order_date::DATE           AS ORDER_DATE,
                    _raw_json:status::VARCHAR(30)        AS STATUS,
                    _raw_json:total_amount::NUMBER(12,2) AS TOTAL_AMOUNT,
                    _raw_json:currency::VARCHAR(3)       AS CURRENCY,
                    _raw_json:created_at::TIMESTAMP_NTZ  AS CREATED_AT,
                    _raw_json:updated_at::TIMESTAMP_NTZ  AS UPDATED_AT,
                    _cdc_operation,
                    _cdc_timestamp,
                    _batch_id
                FROM RAW.PG_ORDERS_RAW
            ) AS src ON tgt.ID = src.ID
            WHEN MATCHED THEN UPDATE SET
                tgt.CUSTOMER_ID     = src.CUSTOMER_ID,
                tgt.STATUS          = src.STATUS,
                tgt.TOTAL_AMOUNT    = src.TOTAL_AMOUNT,
                tgt.UPDATED_AT      = src.UPDATED_AT,
                tgt._CDC_OPERATION  = src._CDC_OPERATION,
                tgt._CDC_TIMESTAMP  = src._CDC_TIMESTAMP,
                tgt._ETL_LOADED_AT  = CURRENT_TIMESTAMP(),
                tgt._ETL_BATCH_ID   = src._BATCH_ID
            WHEN NOT MATCHED THEN INSERT (
                ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, CURRENCY,
                CREATED_AT, UPDATED_AT, _CDC_OPERATION, _CDC_TIMESTAMP,
                _ETL_LOADED_AT, _ETL_BATCH_ID
            ) VALUES (
                src.ID, src.CUSTOMER_ID, src.ORDER_DATE, src.STATUS, src.TOTAL_AMOUNT,
                src.CURRENCY, src.CREATED_AT, src.UPDATED_AT,
                src._CDC_OPERATION, src._CDC_TIMESTAMP,
                CURRENT_TIMESTAMP(), src._BATCH_ID
            );

        ELSE
            RETURN 'ERROR: unknown table ' || TABLE_NAME;
    END CASE;

    RETURN 'OK: merged batch ' || BATCH_ID || ' for table ' || TABLE_NAME;
END;
$$;

GRANT USAGE ON PROCEDURE POSTGRES_CDC.MERGE_CDC_BATCH(VARCHAR, VARCHAR) TO ROLE OPENFLOW_INGEST_ROLE;
