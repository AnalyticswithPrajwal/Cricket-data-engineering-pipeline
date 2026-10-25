-- ============================================================
-- PROJECT: CRICKET DATA ENGINEERING PIPELINE
-- FILE: 03_streams_and_tasks.sql
--
-- PURPOSE:
--   Create a Snowflake stream that tracks new raw-table records
--   and makes them available to the root processing task.
--
-- STREAM:
--   RAW.STREAM_RAW_DATA
--
-- SOURCE TABLE:
--   RAW.RAW_DATA
--
-- APPEND_ONLY:
--   TRUE means the stream tracks inserted rows rather than
--   update and delete changes.
--
-- DOWNSTREAM PROCESSING:
--   DW.TASK_RAW_DATA checks the stream for new data and
--   inserts available records into STAGING.STG_RAW_DATA.
--
-- IMPORTANT:
--   A stream depends on its underlying table. Replacing or
--   dropping the source table can invalidate the stream.
--
--   CREATE OR REPLACE STREAM resets the existing stream
--   object. Do not rerun it casually in a live pipeline,
--   because doing so can reset unconsumed change tracking.
--
-- EXECUTION ORDER:
--   Create this stream before creating DW.TASK_RAW_DATA.
-- ============================================================

USE DATABASE CRICKET_ANALYTICS;
USE SCHEMA RAW;

CREATE STREAM IF NOT EXISTS STREAM_RAW_DATA
    ON TABLE RAW_DATA
    APPEND_ONLY = TRUE;