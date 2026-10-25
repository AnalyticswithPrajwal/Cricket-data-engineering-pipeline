-- ============================================================
-- Project: Cricket Data Engineering Pipeline
-- File: 01_database_and_schemas.sql
-- Purpose:
--   Creates the Snowflake database, schemas, and compute
--   warehouse required by the pipeline.
--
-- Prerequisites:
--   A Snowflake role with permission to create these objects.
--
-- Execution order: 1
-- ============================================================

CREATE DATABASE IF NOT EXISTS CRICKET_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS CRICKET_ANALYTICS.RAW;
CREATE SCHEMA IF NOT EXISTS CRICKET_ANALYTICS.STAGING;
CREATE SCHEMA IF NOT EXISTS CRICKET_ANALYTICS.DW;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE DATABASE CRICKET_ANALYTICS;