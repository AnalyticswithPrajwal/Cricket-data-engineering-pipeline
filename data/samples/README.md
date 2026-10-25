# Sample Cricket Data

## Overview

This directory contains sample cricket match data in JSON format for the **Cricket Data Engineering Pipeline** project.

The sample file represents a men's T20 International match between **West Indies and South Africa** in the ICC Men's T20 World Cup 2024. The match was played on 23 June 2024 at Sir Vivian Richards Stadium in North Sound, Antigua.

The JSON data follows a nested structure containing match details, team and player information, innings, overs, ball-by-ball deliveries, runs, extras, and wickets.

## Sample File

```text
data/
└── samples/
    ├── README.md
    └── sample data.json
```

**File:** `sample data.json`

This file is included to demonstrate the structure of the source data used in the pipeline.

## Data Structure

The JSON file contains the following major sections:

### 1. Metadata (`meta`)

Contains information about the source dataset, including:
- Data version
- Creation date
- Revision number

### 2. Match Information (`info`)

Contains details about the cricket match, including:
- Match date
- Match type and format
- Tournament and group
- Venue and city
- Participating teams
- Number of overs
- Toss winner and decision
- Match winner and result
- Player of the match
- Match officials
- Player registry identifiers

### 3. Teams and Players

The `info.teams` field identifies the participating teams, while `info.players` contains the player lists for each team.

The sample includes:
- West Indies
- South Africa

These details can be used to populate team and player dimension tables in the data warehouse.

### 4. Innings (`innings`)

Contains the innings played by each team.

Each innings includes the batting team and a collection of overs. This structure supports the extraction of innings-level information for analytical reporting.

### 5. Overs and Deliveries

Each innings contains overs, and each over contains individual deliveries.

Delivery-level fields include:
- Batter
- Bowler
- Non-striker
- Runs scored by the batter
- Extras
- Total runs from the delivery
- Wicket information, when applicable
- Review information, when applicable

This structure supports ball-by-ball analysis and the calculation of batting, bowling, and team performance metrics.

### 6. Extras and Wickets

The sample includes delivery-level details for extras such as wides, leg byes, and byes.

Wicket records can include:
- Player dismissed
- Dismissal type
- Fielder information, when available

These fields can support analysis of dismissal types, bowling performance, and match events.

## How This Data Is Used

The sample illustrates the source JSON structure processed by the project's data engineering pipeline.

The intended data flow is:

1. **Amazon S3:** Store cricket match JSON files.
2. **Snowflake External Stage:** Provide access to files stored in S3.
3. **Snowpipe:** Automatically ingest new files into the raw layer.
4. **Raw Layer:** Store the source JSON in a `VARIANT` column, together with its source filename.
5. **Snowflake Stream:** Track new raw records for downstream processing.
6. **Staging Layer:** Extract and transform match, team, player, and innings information into structured tables.
7. **Data Warehouse:** Load the transformed data into analytical tables for querying and reporting.

The sample file is provided for reference and demonstration. The automated pipeline reads files from the configured Amazon S3 location; storing a file in this local repository directory does not automatically load it into Snowflake.

## Purpose of the Sample

This sample helps developers understand:
- The source JSON structure
- The nested relationship between match information, innings, overs, and deliveries
- The fields used for extraction and transformation
- How semi-structured cricket data can be converted into relational tables
- How the data can support downstream cricket analytics

## Notes

- The sample is intended for development, testing, and documentation.
- The JSON structure is nested, so transformations must account for arrays and optional fields.
- Some delivery attributes, such as extras, wickets, and reviews, may not be present for every delivery.
- Keep sample files small and representative where possible.
- Before publishing data to a public repository, confirm that redistribution is permitted by the data source and retain any required attribution or licensing information.
- Do not include AWS credentials, Snowflake passwords, access tokens, or other secrets in this directory.

---
**Project:** Cricket Data Engineering Pipeline  
**Data Format:** JSON  
**Primary Technologies:** Amazon S3, Snowflake, Snowpipe, Streams, and Tasks
