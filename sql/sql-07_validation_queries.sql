-- ============================================================
-- PROJECT: CRICKET DATA ENGINEERING PIPELINE
-- FILE: 07_validation_queries.sql
--
-- PURPOSE:
--   Validate ingestion, transformation, task execution,
--   data quality, and warehouse loading.
--
-- VALIDATION AREAS:
--   1. Raw data availability.
--   2. Snowpipe status.
--   3. Stream status.
--   4. Staging table record counts.
--   5. Data warehouse record counts.
--   6. Sample data inspection.
--   7. Null and duplicate checks.
--   8. Task execution history.
--
-- SAFETY:
--   All queries in this file are read-only.
--   They do not insert, update, delete, or truncate records.
--
-- USAGE:
--   Execute individual sections as needed.
--   Some task-history results depend on your active role
--   and the available monitoring privileges.
-- ============================================================

USE DATABASE CRICKET_ANALYTICS;

-- ============================================================
-- SECTION 1: RAW LAYER VALIDATION
--
-- PURPOSE:
--   Check whether raw JSON records have been ingested.
-- ============================================================

SELECT
    COUNT(*) AS RAW_RECORD_COUNT
FROM CRICKET_ANALYTICS.RAW.RAW_DATA;


-- Inspect sample raw records and their source filenames.

SELECT
    FILENAME,
    RAW_FILE
FROM CRICKET_ANALYTICS.RAW.RAW_DATA
ORDER BY FILENAME
LIMIT 10;


-- Check for missing source filenames.

SELECT
    COUNT(*) AS MISSING_FILENAME_COUNT
FROM CRICKET_ANALYTICS.RAW.RAW_DATA
WHERE FILENAME IS NULL;


-- ============================================================
-- SECTION 2: SNOWPIPE VALIDATION
--
-- PURPOSE:
--   Inspect the configuration and runtime status of the
--   Snowpipe ingestion mechanism.
--
-- INTERPRETATION:
--   A running pipe does not guarantee that files are waiting
--   to load. The pipe may be healthy and idle.
-- ============================================================

SELECT
    SYSTEM$PIPE_STATUS(
        'CRICKET_ANALYTICS.RAW.S3_PIPE'
    ) AS PIPE_STATUS;


-- Display pipe definitions and notification-channel details.
-- Review the results for S3_PIPE.

SHOW PIPES IN SCHEMA CRICKET_ANALYTICS.RAW;


-- ============================================================
-- SECTION 3: STREAM VALIDATION
--
-- PURPOSE:
--   Determine whether the raw stream currently reports
--   change data available for consumption.
--
-- INTERPRETATION:
--   TRUE  = Snowflake reports that stream data is available.
--   FALSE = No stream data is currently reported as available.
--
-- IMPORTANT:
--   This check does not count rows in RAW_DATA. A populated
--   raw table can have an empty stream after consumption.
-- ============================================================

SELECT
    SYSTEM$STREAM_HAS_DATA(
        'CRICKET_ANALYTICS.RAW.STREAM_RAW_DATA'
    ) AS STREAM_HAS_DATA;


-- Inspect the stream definition and its source table.

SHOW STREAMS IN SCHEMA CRICKET_ANALYTICS.RAW;


-- ============================================================
-- SECTION 4: STAGING LAYER RECORD COUNTS
--
-- PURPOSE:
--   Check whether the raw-to-staging and JSON transformation
--   processes have populated their destination tables.
-- ============================================================

SELECT
    'STG_RAW_DATA' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM CRICKET_ANALYTICS.STAGING.STG_RAW_DATA

UNION ALL

SELECT
    'STG_TEAMS',
    COUNT(*)
FROM CRICKET_ANALYTICS.STAGING.STG_TEAMS

UNION ALL

SELECT
    'STG_PLAYERS',
    COUNT(*)
FROM CRICKET_ANALYTICS.STAGING.STG_PLAYERS

UNION ALL

SELECT
    'STG_MATCH_INFO',
    COUNT(*)
FROM CRICKET_ANALYTICS.STAGING.STG_MATCH_INFO

UNION ALL

SELECT
    'STG_MATCH_INNINGS',
    COUNT(*)
FROM CRICKET_ANALYTICS.STAGING.STG_MATCH_INNINGS;


-- Inspect sample extracted teams.

SELECT *
FROM CRICKET_ANALYTICS.STAGING.STG_TEAMS
LIMIT 10;


-- Inspect sample extracted players.

SELECT *
FROM CRICKET_ANALYTICS.STAGING.STG_PLAYERS
LIMIT 10;


-- Inspect sample match metadata.

SELECT *
FROM CRICKET_ANALYTICS.STAGING.STG_MATCH_INFO
LIMIT 10;


-- Inspect sample innings and ball-level records.

SELECT *
FROM CRICKET_ANALYTICS.STAGING.STG_MATCH_INNINGS
LIMIT 10;


-- ============================================================
-- SECTION 5: DATA WAREHOUSE RECORD COUNTS
--
-- PURPOSE:
--   Confirm that the final warehouse tables contain data.
-- ============================================================

SELECT
    'TEAMS' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM CRICKET_ANALYTICS.DW.TEAMS

UNION ALL

SELECT
    'PLAYERS',
    COUNT(*)
FROM CRICKET_ANALYTICS.DW.PLAYERS

UNION ALL

SELECT
    'MATCH_INFO',
    COUNT(*)
FROM CRICKET_ANALYTICS.DW.MATCH_INFO

UNION ALL

SELECT
    'MATCH_INNINGS',
    COUNT(*)
FROM CRICKET_ANALYTICS.DW.MATCH_INNINGS;


-- ============================================================
-- SECTION 6: INSPECT WAREHOUSE DATA
--
-- PURPOSE:
--   Review representative records from each warehouse table.
-- ============================================================

-- Teams and their warehouse identifiers.

SELECT
    TEAM_ID,
    TEAM_NAME,
    TEAM_TYPE
FROM CRICKET_ANALYTICS.DW.TEAMS
ORDER BY TEAM_NAME
LIMIT 20;


-- Players and their associated team identifiers.

SELECT
    PLAYER_ID,
    PLAYER_NAME,
    TEAMS
FROM CRICKET_ANALYTICS.DW.PLAYERS
ORDER BY PLAYER_NAME
LIMIT 20;


-- Match-level information.

SELECT *
FROM CRICKET_ANALYTICS.DW.MATCH_INFO
ORDER BY MATCH_DATE DESC
LIMIT 20;


-- Innings and ball-level information.

SELECT *
FROM CRICKET_ANALYTICS.DW.MATCH_INNINGS
ORDER BY MATCH_ID, INNING_ID, OVER, BALL_IN_OVER
LIMIT 50;


-- ============================================================
-- SECTION 7: DATA QUALITY CHECKS
--
-- PURPOSE:
--   Identify potentially incomplete records in the warehouse.
--
-- NOTE:
--   Some missing values are legitimate in cricket data.
--   For example, a match may have no winner if it was tied
--   or abandoned. Investigate results rather than assuming
--   every NULL represents a pipeline error.
-- ============================================================


-- ------------------------------------------------------------
-- 7A. Teams with missing names
-- ------------------------------------------------------------

SELECT *
FROM CRICKET_ANALYTICS.DW.TEAMS
WHERE TEAM_NAME IS NULL;


-- ------------------------------------------------------------
-- 7B. Players with missing names
-- ------------------------------------------------------------

SELECT *
FROM CRICKET_ANALYTICS.DW.PLAYERS
WHERE PLAYER_NAME IS NULL;


-- ------------------------------------------------------------
-- 7C. Matches with missing source filenames
-- ------------------------------------------------------------

SELECT *
FROM CRICKET_ANALYTICS.DW.MATCH_INFO
WHERE FILENAME IS NULL;


-- ------------------------------------------------------------
-- 7D. Matches with missing primary team associations
--
-- Missing winners and toss details can be legitimate.
-- Missing TEAM1_ID or TEAM2_ID may indicate a mapping issue.
-- ------------------------------------------------------------

SELECT
    MATCH_ID,
    MATCH_DATE,
    TEAM1_ID,
    TEAM2_ID,
    MATCH_WINNER_ID,
    FILENAME
FROM CRICKET_ANALYTICS.DW.MATCH_INFO
WHERE TEAM1_ID IS NULL
   OR TEAM2_ID IS NULL;


-- ------------------------------------------------------------
-- 7E. Check for duplicate team names
-- ------------------------------------------------------------

SELECT
    TEAM_NAME,
    COUNT(*) AS RECORD_COUNT
FROM CRICKET_ANALYTICS.DW.TEAMS
GROUP BY TEAM_NAME
HAVING COUNT(*) > 1
ORDER BY RECORD_COUNT DESC;


-- ------------------------------------------------------------
-- 7F. Check for duplicate player names
--
-- NOTE:
--   This identifies duplicate names, not necessarily
--   duplicate people. Different players can share a name.
-- ------------------------------------------------------------

SELECT
    PLAYER_NAME,
    COUNT(*) AS RECORD_COUNT
FROM CRICKET_ANALYTICS.DW.PLAYERS
GROUP BY PLAYER_NAME
HAVING COUNT(*) > 1
ORDER BY RECORD_COUNT DESC;


-- ------------------------------------------------------------
-- 7G. Check for duplicate match filenames
-- ------------------------------------------------------------

SELECT
    FILENAME,
    COUNT(*) AS RECORD_COUNT
FROM CRICKET_ANALYTICS.DW.MATCH_INFO
WHERE FILENAME IS NOT NULL
GROUP BY FILENAME
HAVING COUNT(*) > 1
ORDER BY RECORD_COUNT DESC;


-- ------------------------------------------------------------
-- 7H. Check for duplicate ball-level keys
--
-- Expected business key:
--   MATCH_ID + INNING_ID + OVER + BALL_IN_OVER
--
-- Multiple rows with the same key may indicate duplicate
-- processing or a problem with the source data.
-- ------------------------------------------------------------

SELECT
    MATCH_ID,
    INNING_ID,
    OVER,
    BALL_IN_OVER,
    COUNT(*) AS RECORD_COUNT
FROM CRICKET_ANALYTICS.DW.MATCH_INNINGS
GROUP BY
    MATCH_ID,
    INNING_ID,
    OVER,
    BALL_IN_OVER
HAVING COUNT(*) > 1
ORDER BY RECORD_COUNT DESC;


-- ============================================================
-- SECTION 8: CHECK TASK DEFINITIONS
--
-- PURPOSE:
--   Inspect the tasks responsible for automatic processing.
--
-- IMPORTANT:
--   Task existence and task execution state are different.
--   A task can exist but be suspended.
-- ============================================================

SHOW TASKS IN SCHEMA CRICKET_ANALYTICS.DW;


-- ============================================================
-- SECTION 9: TASK EXECUTION HISTORY
--
-- PURPOSE:
--   Review recent successful, skipped, and failed task runs.
--
-- LOOK FOR:
--   SUCCEEDED - The task executed successfully.
--   SKIPPED   - A scheduled run did not execute its body,
--               often because a WHEN condition was false.
--   FAILED    - The task execution encountered an error.
--
-- TROUBLESHOOTING:
--   Review ERROR_CODE and ERROR_MESSAGE for failed executions.
--
-- NOTE:
--   Task history visibility depends on the active role and
--   the monitoring privileges available to that role.
-- ============================================================

SELECT
    NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    STATE,
    SCHEDULED_TIME,
    QUERY_START_TIME,
    COMPLETED_TIME,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(
    CRICKET_ANALYTICS.INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START =>
            DATEADD('HOUR', -24, CURRENT_TIMESTAMP()),
        RESULT_LIMIT => 100
    )
)
ORDER BY SCHEDULED_TIME DESC;


-- ============================================================
-- SECTION 10: OVERALL PIPELINE CHECKLIST
--
-- Use the results of the queries above to locate problems.
--
-- RAW TABLE EMPTY:
--   Investigate S3 access, stage configuration, Snowpipe,
--   and file-load history.
--
-- RAW POPULATED BUT STAGING RAW EMPTY:
--   Check stream creation time, stream status, and TASK_RAW_DATA.
--
-- STAGING RAW POPULATED BUT ENTITY TABLES EMPTY:
--   Check staging task definitions, dependencies, and errors.
--
-- STAGING POPULATED BUT DW TABLES EMPTY:
--   Check warehouse task dependencies, task states, joins,
--   and whether source rows satisfy the destination filters.
--
-- DW POPULATED:
--   Inspect sample rows and run the data quality checks.
--
-- IMPORTANT:
--   A successful task execution does not guarantee that rows
--   were inserted. A query may succeed and return zero rows.
-- ============================================================