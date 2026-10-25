# Pipeline Architecture

## Overview

This project implements an automated cricket data engineering pipeline using AWS S3 and Snowflake. It ingests cricket JSON files, transforms nested data into relational tables, and loads curated data into a data warehouse.

## Architecture

```text
Cricket JSON Files
        |
        v
     AWS S3
        |
        v
 S3 Event Notification
        |
        v
     Snowpipe
        |
        v
  RAW.RAW_DATA
        |
        v
 RAW.STREAM_RAW_DATA
        |
        v
 STAGING.STG_RAW_DATA
        |
        v
 Staging Transformation Tasks
        |
        +------------------+
        |        |         |
        v        v         v
      Teams   Players   Match Info
        |
        v
   DW.TEAMS
        |
        v
   DW.PLAYERS
        |
        v
 DW.MATCH_INFO
        |
        v
 DW.MATCH_INNINGS
```

The staging transformations also produce ball-by-ball records in `STAGING.STG_MATCH_INNINGS`.

## Pipeline Components

### 1. AWS S3
Stores source cricket JSON files.

### 2. Snowpipe
Uses S3 event notifications to automatically load new files into Snowflake.

### 3. Raw Layer
Stores the original JSON payload as VARIANT and retains the source filename.

### 4. Stream
Tracks inserted records in the raw table for downstream processing.

### 5. Staging Layer
Extracts teams, players, match information, innings, and delivery-level details from nested JSON.

### 6. Data Warehouse
Stores curated cricket data in relational tables using surrogate identifiers and deduplication logic.

### 7. Snowflake Tasks
Automate the processing workflow and coordinate downstream dependencies.

## Automation

After initial setup, new S3 files are ingested through Snowpipe. Snowflake tasks process available stream data and load the resulting records into staging and warehouse tables.

## Monitoring

The SQL validation script checks record counts, sample data, data quality, pipe status, stream status, and task execution history.
