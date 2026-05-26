-- Table definitions. Each env database is identical in structure.
-- Run against OPENFLOW_DEV, OPENFLOW_TEST, OPENFLOW_PROD.
-- Script uses :db_name variable — set before running:
--   snowsql -v db_name=OPENFLOW_DEV -f 03_tables.sql

USE ROLE SYSADMIN;
USE DATABASE IDENTIFIER($db_name);

-- ============================================================
-- RAW schema: staging tables written by COPY INTO from S3
-- ============================================================

USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS SF_ACCOUNTS_RAW (
    _raw_json       VARIANT,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500),
    _batch_id       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS SF_CONTACTS_RAW (
    _raw_json       VARIANT,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500),
    _batch_id       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS PG_CUSTOMERS_RAW (
    _raw_json       VARIANT,
    _cdc_operation  VARCHAR(10),   -- INSERT | UPDATE | DELETE
    _cdc_timestamp  TIMESTAMP_NTZ,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500),
    _batch_id       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS PG_ORDERS_RAW (
    _raw_json       VARIANT,
    _cdc_operation  VARCHAR(10),
    _cdc_timestamp  TIMESTAMP_NTZ,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500),
    _batch_id       VARCHAR(100)
);

-- ============================================================
-- SALESFORCE schema: typed tables for Salesforce objects
-- ============================================================

USE SCHEMA SALESFORCE;

CREATE TABLE IF NOT EXISTS ACCOUNTS (
    SF_ID               VARCHAR(18)     NOT NULL,
    NAME                VARCHAR(255),
    INDUSTRY            VARCHAR(100),
    ANNUAL_REVENUE      NUMBER(18,2),
    BILLING_CITY        VARCHAR(100),
    BILLING_COUNTRY     VARCHAR(100),
    PHONE               VARCHAR(40),
    WEBSITE             VARCHAR(255),
    NUMBER_OF_EMPLOYEES INTEGER,
    OWNER_ID            VARCHAR(18),
    CREATED_DATE        TIMESTAMP_NTZ,
    LAST_MODIFIED_DATE  TIMESTAMP_NTZ,
    IS_DELETED          BOOLEAN DEFAULT FALSE,
    _ETL_LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _ETL_BATCH_ID       VARCHAR(100),
    CONSTRAINT PK_ACCOUNTS PRIMARY KEY (SF_ID)
);

CREATE TABLE IF NOT EXISTS CONTACTS (
    SF_ID               VARCHAR(18)     NOT NULL,
    ACCOUNT_ID          VARCHAR(18),
    FIRST_NAME          VARCHAR(40),
    LAST_NAME           VARCHAR(80),
    EMAIL               VARCHAR(80),
    PHONE               VARCHAR(40),
    TITLE               VARCHAR(128),
    DEPARTMENT          VARCHAR(80),
    LEAD_SOURCE         VARCHAR(40),
    OWNER_ID            VARCHAR(18),
    CREATED_DATE        TIMESTAMP_NTZ,
    LAST_MODIFIED_DATE  TIMESTAMP_NTZ,
    IS_DELETED          BOOLEAN DEFAULT FALSE,
    _ETL_LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _ETL_BATCH_ID       VARCHAR(100),
    CONSTRAINT PK_CONTACTS PRIMARY KEY (SF_ID)
);

-- ============================================================
-- POSTGRES_CDC schema: CDC target tables
-- ============================================================

USE SCHEMA POSTGRES_CDC;

CREATE TABLE IF NOT EXISTS CUSTOMERS (
    ID                  INTEGER         NOT NULL,
    FIRST_NAME          VARCHAR(100),
    LAST_NAME           VARCHAR(100),
    EMAIL               VARCHAR(255),
    PHONE               VARCHAR(50),
    STATUS              VARCHAR(20),
    CREATED_AT          TIMESTAMP_NTZ,
    UPDATED_AT          TIMESTAMP_NTZ,
    IS_DELETED          BOOLEAN DEFAULT FALSE,
    _CDC_OPERATION      VARCHAR(10),
    _CDC_TIMESTAMP      TIMESTAMP_NTZ,
    _ETL_LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _ETL_BATCH_ID       VARCHAR(100),
    CONSTRAINT PK_CUSTOMERS PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS ORDERS (
    ID                  INTEGER         NOT NULL,
    CUSTOMER_ID         INTEGER,
    ORDER_DATE          DATE,
    STATUS              VARCHAR(30),
    TOTAL_AMOUNT        NUMBER(12,2),
    CURRENCY            VARCHAR(3),
    SHIPPING_ADDRESS    VARCHAR(500),
    NOTES               VARCHAR(1000),
    CREATED_AT          TIMESTAMP_NTZ,
    UPDATED_AT          TIMESTAMP_NTZ,
    IS_DELETED          BOOLEAN DEFAULT FALSE,
    _CDC_OPERATION      VARCHAR(10),
    _CDC_TIMESTAMP      TIMESTAMP_NTZ,
    _ETL_LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _ETL_BATCH_ID       VARCHAR(100),
    CONSTRAINT PK_ORDERS PRIMARY KEY (ID)
);

-- ============================================================
-- External stages (S3 integration)
-- ============================================================

USE SCHEMA RAW;

-- Storage integration must be created first — see 04_roles_grants.sql
CREATE STAGE IF NOT EXISTS OPENFLOW_S3_STAGE
  URL = 's3://openflow-staging-dev-eu-west-2/'
  STORAGE_INTEGRATION = OPENFLOW_S3_INTEGRATION
  FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
  COMMENT = 'S3 staging area for NiFi-loaded files';
