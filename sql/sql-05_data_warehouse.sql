-- ============================================================
-- PROJECT: CRICKET DATA ENGINEERING PIPELINE
-- FILE: 05_data_warehouse.sql
--
-- PURPOSE:
--   Load curated cricket data from the staging layer into
--   the final data warehouse tables.
--
-- WAREHOUSE TABLES:
--   DW.TEAMS         - Team dimension data.
--   DW.PLAYERS       - Player dimension data.
--   DW.MATCH_INFO    - Match-level information.
--   DW.MATCH_INNINGS - Innings and ball-level information.
--
-- TASK DEPENDENCIES:
--   TASK_TEAMS
--       waits for all four staging tasks.
--
--   TASK_PLAYERS
--       runs after TASK_TEAMS.
--
--   TASK_MATCH_INFO
--       runs after TASK_PLAYERS.
--
--   TASK_MATCH_INNINGS
--       runs after TASK_MATCH_INFO.
--
-- DEDUPLICATION:
--   The tasks use existing warehouse records to avoid
--   inserting records that have already been loaded.
--
-- EXECUTION ORDER:
--   Run after the warehouse tables, sequences, and staging
--   tables have been created.
-- ============================================================

USE DATABASE CRICKET_ANALYTICS;
USE SCHEMA DW;

-- ============================================================
-- TASK 1: TASK_TEAMS
--
-- PURPOSE:
--   Insert distinct teams from STAGING.STG_TEAMS into DW.TEAMS.
--
-- TRANSFORMATION:
--   SEQ_TEAMS.NEXTVAL generates a surrogate TEAM_ID.
--
-- DEDUPLICATION:
--   NOT EXISTS checks whether the team name already exists.
--
-- DEPENDENCIES:
--   Waits for all four staging transformation tasks.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_TEAMS
    WAREHOUSE = COMPUTE_WH
    AFTER DW.TASK_STG_TEAMS,
          DW.TASK_STG_PLAYERS,
          DW.TASK_STG_MATCH,
          DW.TASK_STG_MATCH_INNINGS
AS
INSERT INTO DW.TEAMS (
    TEAM_ID,
    TEAM_NAME,
    TEAM_TYPE
)
SELECT
    DW.SEQ_TEAMS.NEXTVAL,
    s.TEAM_NAME,
    s.TEAM_TYPE
FROM (
    SELECT DISTINCT
        TEAM_NAME,
        TEAM_TYPE
    FROM STAGING.STG_TEAMS
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM DW.TEAMS t
    WHERE t.TEAM_NAME = s.TEAM_NAME
);


-- ============================================================
-- TASK 2: TASK_PLAYERS
--
-- PURPOSE:
--   Insert new players and update team associations for
--   existing players.
--
-- TRANSFORMATION:
--   Joins staged player names to the warehouse team table.
--   ARRAY_AGG collects the TEAM_ID values associated with
--   each player.
--
-- MERGE LOGIC:
--   WHEN MATCHED:
--       Merge newly discovered team IDs into the existing
--       player's TEAMS array.
--
--   WHEN NOT MATCHED:
--       Insert a new player using SEQ_PLAYERS.NEXTVAL.
--
-- DEPENDENCY:
--   Runs after TASK_TEAMS so team identifiers are available.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_PLAYERS
    WAREHOUSE = COMPUTE_WH
    AFTER DW.TASK_TEAMS
AS
MERGE INTO DW.PLAYERS target
USING (
    SELECT
        p.PLAYER_NAME,
        ARRAY_AGG(DISTINCT t.TEAM_ID) AS TEAMS
    FROM STAGING.STG_PLAYERS p
    JOIN DW.TEAMS t
        ON p.TEAM_NAME = t.TEAM_NAME
    GROUP BY p.PLAYER_NAME
) source
ON target.PLAYER_NAME = source.PLAYER_NAME

WHEN MATCHED THEN UPDATE SET
    target.TEAMS = ARRAY_DISTINCT(
        ARRAY_CAT(target.TEAMS, source.TEAMS)
    )

WHEN NOT MATCHED THEN INSERT (
    PLAYER_ID,
    PLAYER_NAME,
    TEAMS
)
VALUES (
    DW.SEQ_PLAYERS.NEXTVAL,
    source.PLAYER_NAME,
    source.TEAMS
);


-- ============================================================
-- TASK 3: TASK_MATCH_INFO
--
-- PURPOSE:
--   Load match-level metadata into DW.MATCH_INFO.
--
-- DIMENSION LOOKUPS:
--   TEAM1 and TEAM2 are mapped to TEAM_ID values.
--   TOSS_WINNER is mapped to a team identifier.
--   MATCH_WINNER is mapped to a team identifier.
--   POM is mapped to a player identifier.
--
-- SURROGATE KEY:
--   SEQ_MATCHES.NEXTVAL generates MATCH_ID.
--
-- DEDUPLICATION:
--   FILENAME is used to identify an already-loaded match.
--
-- NULL HANDLING:
--   LEFT JOINs allow missing toss winners, match winners,
--   or player-of-the-match values.
--
-- DEPENDENCY:
--   Runs after TASK_PLAYERS so team and player identifiers
--   are available for the match record.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_MATCH_INFO
    WAREHOUSE = COMPUTE_WH
    AFTER DW.TASK_PLAYERS
AS
INSERT INTO DW.MATCH_INFO (
    MATCH_ID,
    MATCH_TYPE,
    GENDER,
    SEASON,
    CITY,
    VENUE,
    MATCH_DATE,
    TEAM1_ID,
    TEAM2_ID,
    TOSS_WINNER_ID,
    TOSS_DECISION,
    MATCH_WINNER_ID,
    MARGIN,
    EVENT_NAME,
    EVENT_TYPE,
    POM_ID,
    FILENAME
)
SELECT
    DW.SEQ_MATCHES.NEXTVAL,
    s.MATCH_TYPE,
    s.GENDER,
    s.SEASON,
    s.CITY,
    s.VENUE,
    s.MATCH_DATE,
    t1.TEAM_ID,
    t2.TEAM_ID,
    t3.TEAM_ID,
    s.TOSS_DECISION,
    t4.TEAM_ID,
    s.MARGIN,
    s.EVENT_NAME,
    s.EVENT_TYPE,
    p.PLAYER_ID,
    s.FILENAME

FROM STAGING.STG_MATCH_INFO s

JOIN DW.TEAMS t1
    ON s.TEAM1 = t1.TEAM_NAME

JOIN DW.TEAMS t2
    ON s.TEAM2 = t2.TEAM_NAME

LEFT JOIN DW.TEAMS t3
    ON s.TOSS_WINNER = t3.TEAM_NAME

LEFT JOIN DW.TEAMS t4
    ON s.MATCH_WINNER = t4.TEAM_NAME

LEFT JOIN DW.PLAYERS p
    ON s.POM = p.PLAYER_NAME

WHERE NOT EXISTS (
    SELECT 1
    FROM DW.MATCH_INFO m
    WHERE m.FILENAME = s.FILENAME
);


-- ============================================================
-- TASK 4: TASK_MATCH_INNINGS
--
-- PURPOSE:
--   Load staged innings and ball-level records into
--   DW.MATCH_INNINGS.
--
-- MATCH LOOKUP:
--   FILENAME associates each staging record with its
--   corresponding warehouse match.
--
-- LOADED ATTRIBUTES:
--   Match ID, innings ID, over, batting team, ball number,
--   batter, bowler, non-striker, runs, extras, dismissal
--   information, and powerplay indicator.
--
-- DEDUPLICATION:
--   Checks the combination of MATCH_ID, INNING_ID, OVER,
--   and BALL_IN_OVER before inserting a record.
--
-- DEPENDENCY:
--   Runs after TASK_MATCH_INFO so MATCH_ID values exist.
--
-- IMPORTANT:
--   Confirm that the DW column types agree with the values
--   inserted from staging, especially BATTING_TEAM.
-- ============================================================

CREATE OR REPLACE TASK DW.TASK_MATCH_INNINGS
    WAREHOUSE = COMPUTE_WH
    AFTER DW.TASK_MATCH_INFO
AS
INSERT INTO DW.MATCH_INNINGS (
    MATCH_ID,
    INNING_ID,
    OVER,
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
    IS_POWERPLAY
)
SELECT
    m.MATCH_ID,
    s.INNING_ID,
    s.OVER_NO,
    s.BATTING_TEAM,
    s.BALL_IN_OVER,
    s.BATTER,
    s.BOWLER,
    s.NON_STRIKER,
    s.RUNS_BATTER,
    s.EXTRA_TYPE,
    s.RUNS_EXTRAS,
    s.RUNS_TOTAL,
    s.DISMISSAL_KIND,
    s.OUT_PLAYER,
    s.IS_POWERPLAY

FROM STAGING.STG_MATCH_INNINGS s

JOIN DW.MATCH_INFO m
    ON s.FILENAME = m.FILENAME

WHERE NOT EXISTS (
    SELECT 1
    FROM DW.MATCH_INNINGS existing
    WHERE existing.MATCH_ID = m.MATCH_ID
      AND existing.INNING_ID = s.INNING_ID
      AND existing.OVER = s.OVER_NO
      AND existing.BALL_IN_OVER = s.BALL_IN_OVER
);