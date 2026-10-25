-- ============================================================
-- Project: Cricket Data Engineering Pipeline
-- File: 02_raw_layer.sql
-- Purpose:
--   Configure the Snowflake raw ingestion layer using AWS S3 and Snowpipe auto-ingest.
--
-- Components:
--   1. S3_INT       - Storage integration with AWS IAM.
--   2. S3_EXT_STAGE - External stage referencing S3 JSON files.
--   3. RAW_DATA     - Table storing JSON and source filenames.
--   4. S3_PIPE      - Automatically loads new files into RAW_DATA.
--
-- Prerequisites:
--   - An AWS S3 bucket containing cricket JSON files.
--   - An AWS IAM role configured for Snowflake access.
--   - Appropriate Snowflake privileges.
--   - An S3 event notification configured for Snowpipe auto-ingest.
--
-- IMPORTANT:
--   Replace all <PLACEHOLDERS> with your own configuration.
--   Never commit AWS access keys, secret keys, passwords, or tokens.
--
-- Execution order: 2
-- ============================================================

USE DATABASE CRICKET_ANALYTICS;
USE SCHEMA RAW;

-- ============================================================
-- STEP 1: CREATE THE RAW DATA TABLE
-- Purpose:
--   Store the original JSON payload and its source filename.
--
-- RAW_FILE: JSON payload stored as Snowflake VARIANT.
-- FILENAME: Name/path of the source file in S3.
-- ============================================================

CREATE TABLE IF NOT EXISTS RAW_DATA (
    RAW_FILE VARIANT,
    FILENAME VARCHAR(100)
);

-- ============================================================
-- STEP 2: CREATE THE AWS STORAGE INTEGRATION
-- Purpose:
--   Allow Snowflake to access the configured S3 location through an AWS IAM role.
--
-- SECURITY:
--   The IAM role ARN is an identifier, not an AWS secret key.
--   Use a placeholder in the public GitHub version.
--
-- NOTE:
--   CREATE OR REPLACE can replace an existing integration.
--   Run this during initial setup, not during routine ingestion.
-- ============================================================

CREATE OR REPLACE STORAGE INTEGRATION S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN =
        'arn:aws:iam::<AWS_ACCOUNT_ID>:role/<SNOWFLAKE_IAM_ROLE>'
    STORAGE_ALLOWED_LOCATIONS = (
        's3://<S3_BUCKET_NAME>/cricket/'
    );

-- ============================================================
-- STEP 3: INSPECT THE STORAGE INTEGRATION
-- Purpose:
--   Retrieve Snowflake-generated configuration values needed
--   to complete the AWS IAM trust relationship.
--
-- ACTION:
--   Follow Snowflake's AWS configuration instructions and
--   configure the IAM role trust policy as required.
-- ============================================================

DESC INTEGRATION S3_INT;

-- ============================================================
-- STEP 4: CREATE THE EXTERNAL STAGE
-- Purpose:
--   Reference the cricket JSON files stored in S3.
--
-- The storage integration provides the configured access.
-- The JSON file format tells Snowflake how to interpret files.
-- ============================================================

CREATE OR REPLACE STAGE S3_EXT_STAGE
    STORAGE_INTEGRATION = S3_INT
    URL = 's3://<S3_BUCKET_NAME>/cricket/'
    FILE_FORMAT = (
        TYPE = JSON
    );

-- ============================================================
-- STEP 5: VERIFY FILES IN THE EXTERNAL STAGE
-- Purpose:
--   List files visible through the configured S3 stage.
--
-- This is a diagnostic command; it is not required for
-- every scheduled pipeline execution.
-- ============================================================

LIST @S3_EXT_STAGE;

-- ============================================================
-- STEP 6: CREATE THE SNOWPIPE AUTO-INGEST PIPE
-- Purpose:
--   Load new JSON files from the external stage into RAW_DATA.
--
-- AUTO_INGEST = TRUE:
--   Enables notification-driven ingestion when the AWS
--   notification configuration is correctly established.
--
-- METADATA$FILENAME:
--   Records the source file path for traceability.
--
-- ON_ERROR = CONTINUE:
--   Allows loading to continue when individual records
--   encounter errors. Monitor load history for rejected data.
--
-- IMPORTANT:
--   Snowflake generates a notification channel for the pipe.
--   Configure the S3 event notification to use the correct
--   Snowflake-provided SQS destination and required permissions.
-- ============================================================

CREATE OR REPLACE PIPE S3_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO RAW_DATA (
    RAW_FILE,
    FILENAME
)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @S3_EXT_STAGE
)
FILE_FORMAT = (
    TYPE = JSON
)
ON_ERROR = CONTINUE;

-- ============================================================
-- STEP 7: VERIFY THE PIPE CONFIGURATION
-- Purpose:
--   Inspect the pipe definition and notification channel.
-- ============================================================

SHOW PIPES IN SCHEMA CRICKET_ANALYTICS.RAW;