# Data Model

## Overview

The project uses a layered data architecture consisting of raw ingestion, staging transformations, and a curated data warehouse.

## 1. Raw Layer

### `RAW.RAW_DATA`

Stores the original source JSON documents.

- `RAW_FILE`: JSON document stored as VARIANT.
- `FILENAME`: Source filename or path.

### `RAW.STREAM_RAW_DATA`

Tracks inserted records from `RAW.RAW_DATA` for downstream processing.

## 2. Staging Layer

### `STAGING.STG_RAW_DATA`

Retains raw JSON records for transformation.

### `STAGING.STG_TEAMS`

Contains extracted team names and team types.

### `STAGING.STG_PLAYERS`

Contains player names and associated team names.

### `STAGING.STG_MATCH_INFO`

Contains match metadata, including date, venue, teams, toss information, result, event details, and source filename.

### `STAGING.STG_MATCH_INNINGS`

Contains flattened innings and delivery-level details, including batter, bowler, runs, extras, dismissals, and powerplay indicators.

## 3. Data Warehouse

### `DW.TEAMS`

Stores team records with generated team identifiers.

### `DW.PLAYERS`

Stores player records and their associated team identifiers.

### `DW.MATCH_INFO`

Stores match-level records and references to the participating teams, toss winner, match winner, and player of the match where available.

### `DW.MATCH_INNINGS`

Stores ball-level match data linked to the corresponding warehouse match.

## 4. Data Flow

Raw JSON documents are ingested from S3, transformed into structured staging tables, and loaded into warehouse tables through Snowflake tasks.

Source filenames support traceability between the raw, staging, and warehouse layers.

## 5. Data Quality

The validation queries check for missing values, duplicate business keys, row counts, and unsuccessful task executions.

The warehouse uses surrogate identifiers for teams, players, and matches.
