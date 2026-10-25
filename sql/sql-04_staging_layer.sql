-- ============================================================
-- PROJECT: CRICKET DATA ENGINEERING PIPELINE
-- FILE: 04_staging_layer.sql
--
-- PURPOSE:
--   Create Snowflake tasks that process raw cricket JSON data
--   and populate structured staging tables.
--
-- STAGING TABLES:
--   STG_RAW_DATA       - Accumulated raw JSON records.
--   STG_TEAMS          - Extracted team information.
--   STG_PLAYERS        - Extracted player and team information.
--   STG_MATCH_INFO     - Extracted match-level information.
--   STG_MATCH_INNINGS  - Extracted innings and delivery details.
--
-- AUTOMATION:
--   TASK_RAW_DATA runs when the raw stream contains new data.
--   The four staging tasks depend on TASK_RAW_DATA.
--
-- IMPORTANT:
--   STG_RAW_DATA is not truncated by the root task. This allows
--   records from multiple ingestion batches to accumulate.
--
-- EXECUTION ORDER:
--   Run after the raw tables and stream have been created.
-- ============================================================

USE DATABASE CRICKET_ANALYTICS;
USE SCHEMA STAGING;


-- ============================================================
-- TASK 1: TASK_RAW_DATA
--
-- PURPOSE:
--   Transfer new records detected by STREAM_RAW_DATA into
--   STAGING.STG_RAW_DATA.
--
-- SCHEDULE:
--   Snowflake checks the task every minute.
--
-- CONDITION:
--   SYSTEM$STREAM_HAS_DATA returns TRUE when the stream has
--   change data available for processing.
--
-- WHY THERE IS NO TRUNCATE:
--   The staging raw table retains records across ingestion
--   batches instead of keeping only the latest batch.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_RAW_DATA
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA(
        'CRICKET_ANALYTICS.RAW.STREAM_RAW_DATA'
    )
AS
BEGIN
    INSERT INTO CRICKET_ANALYTICS.STAGING.STG_RAW_DATA
        (RAW_FILE, FILENAME)
    SELECT
        RAW_FILE,
        FILENAME
    FROM CRICKET_ANALYTICS.RAW.STREAM_RAW_DATA;
END;


-- ============================================================
-- TASK 2: TASK_STG_TEAMS
--
-- PURPOSE:
--   Extract team names and team type from the raw JSON.
--
-- JSON SOURCE:
--   RAW_FILE:info.teams contains the teams associated with
--   a match.
--
-- TRANSFORMATION:
--   LATERAL FLATTEN expands the JSON team array into rows.
--   DISTINCT removes duplicate team/type combinations from
--   the current staging refresh.
--
-- OUTPUT:
--   STAGING.STG_TEAMS
--
-- NOTE:
--   This task truncates and rebuilds its staging output from
--   all records currently in STG_RAW_DATA.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_STG_TEAMS
    WAREHOUSE = COMPUTE_WH
    AFTER DW.TASK_RAW_DATA
AS
BEGIN
    TRUNCATE TABLE CRICKET_ANALYTICS.STAGING.STG_TEAMS;

    INSERT INTO CRICKET_ANALYTICS.STAGING.STG_TEAMS
        (TEAM_NAME, TEAM_TYPE)
    SELECT DISTINCT
        f0.VALUE::VARCHAR AS TEAM_NAME,
        r.RAW_FILE:info.team_type::VARCHAR AS TEAM_TYPE
    FROM CRICKET_ANALYTICS.STAGING.STG_RAW_DATA r,
         LATERAL FLATTEN(
             INPUT => r.RAW_FILE:info.teams
         ) f0;
END;


-- ============================================================
-- TASK 3: TASK_STG_PLAYERS
--
-- PURPOSE:
--   Extract player names and their associated team names.
--
-- JSON SOURCE:
--   RAW_FILE:info.players contains player lists grouped
--   by team.
--
-- TRANSFORMATION:
--   The first FLATTEN expands the team-keyed player object.
--   The second FLATTEN expands each team's player array.
--
-- OUTPUT:
--   STAGING.STG_PLAYERS
--
-- RESULT:
--   PLAYER_NAME and TEAM_NAME are stored as relational values.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_STG_PLAYERS
    WAREHOUSE = COMPUTE_WH
    AFTER DW.TASK_RAW_DATA
AS
BEGIN
    TRUNCATE TABLE CRICKET_ANALYTICS.STAGING.STG_PLAYERS;

    INSERT INTO CRICKET_ANALYTICS.STAGING.STG_PLAYERS
        (PLAYER_NAME, TEAM_NAME)
    SELECT DISTINCT
        f1.VALUE::VARCHAR AS PLAYER_NAME,
        f0.KEY::VARCHAR AS TEAM_NAME
    FROM CRICKET_ANALYTICS.STAGING.STG_RAW_DATA r,
         LATERAL FLATTEN(
             INPUT => r.RAW_FILE:info.players
         ) f0,
         LATERAL FLATTEN(
             INPUT => f0.VALUE
         ) f1;
END;


-- ============================================================
-- TASK 4: TASK_STG_MATCH
--
-- PURPOSE:
--   Extract match-level metadata from the raw JSON.
--
-- FIELDS EXTRACTED:
--   Match type, gender, season, city, venue, match date,
--   both teams, toss details, match outcome, event details,
--   team type, player of the match, and source filename.
--
-- TRANSFORMATIONS:
--   TRY_TO_DATE converts the first match date into a DATE.
--   CASE converts the outcome into a readable margin string.
--   Examples include "5 wickets", "20 runs", or "no result".
--
-- OUTPUT:
--   STAGING.STG_MATCH_INFO
--
-- SOURCE TRACKING:
--   FILENAME is retained to associate staging match records
--   with the original source file.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_STG_MATCH
    WAREHOUSE = COMPUTE_WH
    AFTER DW.TASK_RAW_DATA
AS
BEGIN
    TRUNCATE TABLE
        CRICKET_ANALYTICS.STAGING.STG_MATCH_INFO;

    INSERT INTO CRICKET_ANALYTICS.STAGING.STG_MATCH_INFO
    SELECT
        r.RAW_FILE:info.match_type::VARCHAR,
        r.RAW_FILE:info.gender::VARCHAR,
        r.RAW_FILE:info.season::VARCHAR,
        r.RAW_FILE:info.city::VARCHAR,
        r.RAW_FILE:info.venue::VARCHAR,
        TRY_TO_DATE(
            r.RAW_FILE:info.dates[0]::VARCHAR
        ),
        r.RAW_FILE:info.teams[0]::VARCHAR,
        r.RAW_FILE:info.teams[1]::VARCHAR,
        r.RAW_FILE:info.toss.winner::VARCHAR,
        r.RAW_FILE:info.toss.decision::VARCHAR,
        r.RAW_FILE:info.outcome.winner::VARCHAR,

        CASE
            WHEN r.RAW_FILE:info.outcome.by.wickets
                 IS NOT NULL
            THEN
                r.RAW_FILE:info.outcome.by.wickets::VARCHAR
                || ' wickets'

            WHEN r.RAW_FILE:info.outcome.by.runs
                 IS NOT NULL
            THEN
                r.RAW_FILE:info.outcome.by.runs::VARCHAR
                || ' runs'

            WHEN r.RAW_FILE:info.outcome.result
                 IS NOT NULL
            THEN
                r.RAW_FILE:info.outcome.result::VARCHAR

            ELSE 'no result'
        END,

        r.RAW_FILE:info.event.name::VARCHAR,
        r.RAW_FILE:info.team_type::VARCHAR,
        r.RAW_FILE:info.player_of_match[0]::VARCHAR,
        r.FILENAME

    FROM CRICKET_ANALYTICS.STAGING.STG_RAW_DATA r;
END;


-- ============================================================
-- TASK 5: TASK_STG_MATCH_INNINGS
--
-- PURPOSE:
--   Flatten nested innings, overs, and deliveries into
--   individual ball-level staging records.
--
-- JSON ARRAYS:
--   innings -> overs -> deliveries
--
-- TRANSFORMATIONS:
--   INNING_ID     - Innings array index plus one.
--   OVER_NO       - Over number plus one.
--   BALL_IN_OVER  - Delivery array index plus one.
--   RUNS_BATTER   - Runs credited to the batter.
--   RUNS_EXTRAS   - Extra runs.
--   RUNS_TOTAL    - Total runs scored from the delivery.
--   EXTRA_TYPE    - First key found in the extras object.
--   DISMISSAL_KIND and OUT_PLAYER - First wicket details.
--   PP_START and PP_END - Powerplay boundaries.
--   IS_POWERPLAY  - 1 if the over falls in the configured
--                   powerplay range; otherwise 0.
--
-- POWERPLAY LOGIC:
--   Uses innings-level powerplay boundaries when available.
--   Otherwise, falls back to DEFAULT_POWERPLAYS by match type.
--
-- OUTPUT:
--   STAGING.STG_MATCH_INNINGS
--
-- SOURCE TRACKING:
--   FILENAME identifies the match's source JSON file.
--
-- IMPORTANT:
--   This task rebuilds the staging innings table from all
--   records currently stored in STG_RAW_DATA.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_STG_MATCH_INNINGS
    WAREHOUSE = COMPUTE_WH
    AFTER DW.TASK_RAW_DATA
AS
BEGIN
    TRUNCATE TABLE
        CRICKET_ANALYTICS.STAGING.STG_MATCH_INNINGS;

    INSERT INTO CRICKET_ANALYTICS.STAGING.STG_MATCH_INNINGS
    WITH delivery_rows AS (
        SELECT
            r.FILENAME,
            r.RAW_FILE:info.match_type::VARCHAR AS MATCH_TYPE,

            inn.INDEX + 1 AS INNING_ID,
            ov.VALUE:over::INT + 1 AS OVER_NO,

            inn.VALUE:team::VARCHAR AS BATTING_TEAM,
            d.INDEX + 1 AS BALL_IN_OVER,

            d.VALUE:batter::VARCHAR AS BATTER,
            d.VALUE:bowler::VARCHAR AS BOWLER,
            d.VALUE:non_striker::VARCHAR AS NON_STRIKER,

            d.VALUE:runs.batter::INT AS RUNS_BATTER,

            OBJECT_KEYS(
                d.VALUE:extras
            )[0]::VARCHAR AS EXTRA_TYPE,

            COALESCE(
                d.VALUE:runs.extras::INT,
                0
            ) AS RUNS_EXTRAS,

            d.VALUE:runs.total::INT AS RUNS_TOTAL,

            d.VALUE:wickets[0].kind::VARCHAR
                AS DISMISSAL_KIND,

            d.VALUE:wickets[0].player_out::VARCHAR
                AS OUT_PLAYER,

            COALESCE(
                CEIL(
                    inn.VALUE:powerplays[0].from::FLOAT
                ),
                pp.START_OVER
            ) AS PP_START,

            COALESCE(
                CEIL(
                    inn.VALUE:powerplays[0].to::FLOAT
                ),
                pp.END_OVER
            ) AS PP_END

        FROM CRICKET_ANALYTICS.STAGING.STG_RAW_DATA r

        LEFT JOIN CRICKET_ANALYTICS.RAW.DEFAULT_POWERPLAYS pp
            ON r.RAW_FILE:info.match_type::VARCHAR
               = pp.MATCH_TYPE,

             LATERAL FLATTEN(
                 INPUT => r.RAW_FILE:innings
             ) inn,

             LATERAL FLATTEN(
                 INPUT => inn.VALUE:overs
             ) ov,

             LATERAL FLATTEN(
                 INPUT => ov.VALUE:deliveries
             ) d
    )

    SELECT
        INNING_ID,
        OVER_NO,
        BATTING_TEAM,
        BALL_IN_OVER,
        BATTER,
        BOWLER,
        NON_STRIKER,
        RUNS_BATTER,
        EXTRA_TYPE,
        RUNS_EXTRAS,
        RUNS_TOTAL,
        DISMISSAL_KIND,
        OUT_PLAYER,
        PP_START,
        PP_END,

        IFF(
            OVER_NO BETWEEN PP_START AND PP_END,
            1,
            0
        ) AS IS_POWERPLAY,

        FILENAME

    FROM delivery_rows;
END;