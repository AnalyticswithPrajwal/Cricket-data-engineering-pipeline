-- ============================================================
-- PROJECT: CRICKET DATA ENGINEERING PIPELINE
-- FILE: 06_initial_load.sql
--
-- PURPOSE:
--   Perform the initial historical-data load from S3 into
--   the Snowflake raw ingestion table.
--
-- DATA FLOW:
--   AWS S3
--      |
--      v
--   RAW.S3_EXT_STAGE
--      |
--      v
--   RAW.RAW_DATA
--      |
--      v
--   RAW.STREAM_RAW_DATA
--      |
--      v
--   DW.TASK_RAW_DATA
--      |
--      v
--   STAGING TABLES
--      |
--      v
--   DW TABLES
--
-- PREREQUISITES:
--   1. RAW.RAW_DATA exists.
--   2. RAW.S3_EXT_STAGE exists and can access S3.
--   3. RAW.STREAM_RAW_DATA exists on RAW.RAW_DATA.
--   4. The task definitions have been created.
--   5. Historical files are available in the S3 stage.
--
-- IMPORTANT:
--   This script is intended for initial setup.
--   Do not use FORCE = TRUE casually: it can reload files
--   that Snowflake has already loaded.
--
--   Do not truncate production tables as part of a routine
--   initial-load or validation procedure.
-- ============================================================


-- ============================================================
-- STEP 1: SELECT THE PROJECT DATABASE AND SCHEMA
-- ============================================================

USE DATABASE CRICKET_ANALYTICS;
USE SCHEMA RAW;


-- ============================================================
-- STEP 2: VERIFY THAT THE EXTERNAL STAGE CAN SEE THE FILES
--
-- PURPOSE:
--   List the historical JSON files available through the
--   configured external stage.
--
-- EXPECTED RESULT:
--   A list of files under the configured S3 cricket prefix.
--
-- TROUBLESHOOTING:
--   If the expected files are missing, check the S3 path,
--   storage integration, IAM role, and file permissions.
-- ============================================================

LIST @CRICKET_ANALYTICS.RAW.S3_EXT_STAGE;


-- ============================================================
-- STEP 3: LOAD HISTORICAL JSON FILES INTO THE RAW TABLE
--
-- PURPOSE:
--   Load eligible files from the S3 stage into RAW.RAW_DATA.
--
-- RAW_FILE:
--   Stores the parsed JSON document as VARIANT.
--
-- FILENAME:
--   Stores the source file path for traceability.
--
-- LOAD BEHAVIOUR:
--   Snowflake normally tracks files loaded by COPY INTO
--   and skips files already recorded in its load metadata.
--
-- ERROR HANDLING:
--   ON_ERROR = CONTINUE allows the load to continue when
--   individual records encounter errors. Review load history
--   afterward to identify any files or records that failed.
--
-- IMPORTANT:
--   If Snowpipe has already loaded the historical files,
--   this command may load zero additional rows.
-- ============================================================

COPY INTO CRICKET_ANALYTICS.RAW.RAW_DATA (
    RAW_FILE,
    FILENAME
)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @CRICKET_ANALYTICS.RAW.S3_EXT_STAGE
)
FILE_FORMAT = (
    TYPE = JSON
)
ON_ERROR = CONTINUE;


-- ============================================================
-- STEP 4: VERIFY THE RAW TABLE
--
-- PURPOSE:
--   Confirm that raw JSON records are available.
-- ============================================================

SELECT
    COUNT(*) AS RAW_RECORD_COUNT
FROM CRICKET_ANALYTICS.RAW.RAW_DATA;


-- ============================================================
-- STEP 5: INSPECT RECENT RAW RECORDS
--
-- PURPOSE:
--   Confirm that JSON documents and source filenames have
--   been captured correctly.
-- ============================================================

SELECT
    FILENAME,
    RAW_FILE
FROM CRICKET_ANALYTICS.RAW.RAW_DATA
ORDER BY FILENAME
LIMIT 10;


-- ============================================================
-- STEP 6: CHECK THE STREAM STATUS
--
-- PURPOSE:
--   Inspect whether the stream has change data available.
--
-- IMPORTANT:
--   SYSTEM$STREAM_HAS_DATA returning FALSE does not mean
--   the raw table is empty. It means the stream does not
--   currently report data available for consumption.
--
--   If the historical records were loaded before the stream
--   was created, those existing rows will not automatically
--   become new stream records.
-- ============================================================

SELECT SYSTEM$STREAM_HAS_DATA(
    'CRICKET_ANALYTICS.RAW.STREAM_RAW_DATA'
) AS STREAM_HAS_DATA;


-- ============================================================
-- STEP 7: REVIEW THE PIPE STATUS
--
-- PURPOSE:
--   Check the status of the Snowpipe auto-ingest mechanism.
--
-- NOTE:
--   Pipe status and the stream's change-data status are
--   different things. A running pipe does not guarantee
--   that the stream currently contains unconsumed rows.
-- ============================================================

SELECT SYSTEM$PIPE_STATUS(
    'CRICKET_ANALYTICS.RAW.S3_PIPE'
) AS PIPE_STATUS;


-- ============================================================
-- NEXT STEPS
--
-- For a fresh environment:
--   1. Confirm the initial files were loaded into RAW_DATA.
--   2. Confirm the stream was created before the initial load.
--   3. Enable the task graph only after its task definitions
--      and dependencies have been verified.
--   4. Check task history and the staging/DW row counts.
--
-- For an existing environment:
--   Do not assume this COPY command will replay previously
--   loaded records through the stream. If RAW_DATA already
--   contains historical records, use a separate, controlled
--   bootstrap procedure to process those records.
-- ============================================================