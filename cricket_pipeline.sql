/*
================================================================================
CRICKET ANALYTICS PIPELINE - SNOWFLAKE SQL
================================================================================
Purpose:
  Build a layered cricket analytics warehouse from Cricsheet JSON files:
  LAND stage -> RAW JSON -> CLEAN relational tables -> CONSUMPTION star schema
  -> reporting views -> data-quality checks -> pipeline audit task.

Execution requirements:
  1. Upload unzipped Cricsheet .json files to @CRICKET.LAND.MY_STG before running.
  2. Run with a role that can create databases, schemas, tables, views and tasks.
  3. This is a full-rebuild script. CREATE OR REPLACE statements overwrite the
     current contents of the named tables/views. Back up anything you need first.
  4. The stage itself is preserved. The RAW table is rebuilt, so staged files are
     loaded again after a full run.
  5. The audit task is created suspended. Resume it only after checking its run.
  6. The task records audit counts only; it does not execute the ETL pipeline.
  7. This script has been statically reviewed, but it must be executed against
     your Snowflake account and representative source files to confirm runtime
     compatibility and data-quality results. No script can honestly be called
     100% verified without that execution.

Data assumptions:
  - The first team in info.teams is Team A; the second is Team B. This is not
    necessarily the team that bats first.
  - Match-level totals exclude super overs and aggregate a team's innings.
  - Up to two wicket records per delivery are retained.
  - Bowler runs conceded = batter runs + wides + no-balls; byes, leg-byes and
    penalty runs are excluded.
  - For filenames without a numeric Cricsheet ID, a hash fallback is used.
================================================================================
*/

/* SECTION 1 - DATABASE, SCHEMAS AND WAREHOUSE. */
CREATE DATABASE IF NOT EXISTS cricket;

CREATE SCHEMA IF NOT EXISTS cricket.land;         
CREATE SCHEMA IF NOT EXISTS cricket.raw;          
CREATE SCHEMA IF NOT EXISTS cricket.clean;        
CREATE SCHEMA IF NOT EXISTS cricket.consumption;  

SHOW SCHEMAS IN DATABASE cricket;


CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE cricket;


/* SECTION 2 - LAND LAYER: JSON FILE FORMAT AND STAGE. */
/* Configure JSON parsing for staged source files. */
CREATE OR REPLACE FILE FORMAT cricket.land.my_json_format
    TYPE = JSON;

/* Do not replace the stage: doing so can remove staged-file metadata/content. */
CREATE STAGE IF NOT EXISTS cricket.land.my_stg;


LIST @cricket.land.my_stg;


/* SECTION 3 - RAW LAYER: STORE ORIGINAL JSON AND METADATA. */
CREATE OR REPLACE TRANSIENT TABLE cricket.raw.match_raw_tb1 (
    meta                OBJECT,
    info                VARIANT NOT NULL,
    innings             ARRAY,                
    stg_file_name       TEXT    NOT NULL,
    stg_file_row_number INT     NOT NULL,
    stg_file_hashkey    TEXT    NOT NULL,
    stg_modified_ts     TIMESTAMP NOT NULL
);


/* Load source JSON components and file metadata into RAW. */
COPY INTO cricket.raw.match_raw_tb1
FROM (
    SELECT
        $1:meta,
        $1:info,
        $1:innings,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_CONTENT_KEY,
        METADATA$FILE_LAST_MODIFIED
    FROM @cricket.land.my_stg
)
FILE_FORMAT = (FORMAT_NAME = 'cricket.land.my_json_format')
PATTERN = '.*[.]json'
ON_ERROR = 'CONTINUE';


SELECT COUNT(*) AS raw_match_files FROM cricket.raw.match_raw_tb1;


/* Derive a match ID from the filename and keep one latest row per ID. */
CREATE OR REPLACE VIEW cricket.raw.match_raw_vw AS
SELECT
    meta, info, innings,
    stg_file_name, stg_file_row_number, stg_file_hashkey, stg_modified_ts,
    match_id
FROM (
    SELECT
        r.*,
        COALESCE(
            REGEXP_SUBSTR(r.stg_file_name, $$([0-9]+)[.]json$$, 1, 1, 'e', 1)::NUMBER,
            ABS(HASH(r.stg_file_name))      
        ) AS match_id
    FROM cricket.raw.match_raw_tb1 r
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY stg_modified_ts DESC) = 1;


/* SECTION 4 - CLEAN LAYER: MATCH DETAILS. */
CREATE OR REPLACE TRANSIENT TABLE cricket.clean.match_detail_clean AS
SELECT
    match_id,
    info:match_type_number::INT                         AS match_type_number,
    COALESCE(info:event.name::TEXT, 'NA')               AS event_name,

    
    CASE
        WHEN info:event.match_number::TEXT IS NOT NULL THEN info:event.match_number::TEXT
        WHEN info:event.stage::TEXT        IS NOT NULL THEN info:event.stage::TEXT
        ELSE 'NA'
    END                                                 AS match_stage,

    info:dates[0]::DATE                                 AS event_date,
    YEAR(info:dates[0]::DATE)                           AS event_year,
    MONTH(info:dates[0]::DATE)                          AS event_month,
    DAY(info:dates[0]::DATE)                            AS event_day,

    info:match_type::TEXT                               AS match_type,        
    info:season::TEXT                                   AS season,
    info:team_type::TEXT                                AS team_type,         
    info:overs::INT                                     AS total_overs,       
    COALESCE(info:balls_per_over::INT, 6)               AS balls_per_over,    
    COALESCE(info:city::TEXT, 'NA')                     AS city,              
    COALESCE(info:venue::TEXT, 'NA')                    AS venue,
    COALESCE(info:gender::TEXT, 'NA')                   AS gender,            

    info:teams[0]::TEXT                                 AS first_team,        
    info:teams[1]::TEXT                                 AS second_team,

    CASE
        WHEN info:outcome.winner::TEXT IS NOT NULL      THEN 'Result Declared'
        WHEN info:outcome.result::TEXT = 'tie'          THEN 'Tie'
        WHEN info:outcome.result::TEXT = 'no result'    THEN 'No Result'
        WHEN info:outcome.result::TEXT = 'draw'         THEN 'Draw'
        ELSE COALESCE(info:outcome.result::TEXT, 'NA')
    END                                                 AS match_result,
    COALESCE(info:outcome.winner::TEXT, 'NA')           AS winner,
    info:outcome.by.runs::INT                           AS win_by_runs,
    info:outcome.by.wickets::INT                        AS win_by_wickets,

    COALESCE(info:toss.winner::TEXT, 'NA')              AS toss_winner,
    COALESCE(INITCAP(info:toss.decision::TEXT), 'NA')   AS toss_decision,

    info:officials.match_referees[0]::TEXT              AS referee_name,      

    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
FROM cricket.raw.match_raw_vw;


CREATE OR REPLACE TABLE cricket.clean.player_clean_tb1 AS
SELECT
    r.match_id,
    COALESCE(r.info:gender::TEXT, 'NA')                 AS gender,
    p.key::TEXT                                         AS team_name,
    pl.value::TEXT                                      AS player_name,
    COALESCE(GET(r.info:registry.people, pl.value::TEXT)::TEXT,
             pl.value::TEXT)                            AS player_registry_id,
    r.stg_file_name,
    r.stg_file_row_number,
    r.stg_file_hashkey,
    r.stg_modified_ts
FROM cricket.raw.match_raw_vw r,
     LATERAL FLATTEN(INPUT => r.info:players) p,      
     LATERAL FLATTEN(INPUT => p.value) pl;            


/* Flatten innings/overs/deliveries without multiplying rows by wicket count. */
CREATE OR REPLACE TRANSIENT TABLE cricket.clean.delivery_clean AS
WITH base AS (
    SELECT
        m.match_id,
        COALESCE(m.info:gender::TEXT, 'NA')                         AS gender,
        i.index + 1                                                 AS innings_no,
        i.value:team::TEXT                                          AS batting_team,
        IFF(i.value:team::TEXT = m.info:teams[0]::TEXT,
            m.info:teams[1]::TEXT, m.info:teams[0]::TEXT)           AS bowling_team,
        COALESCE(i.value:super_over::BOOLEAN, FALSE)                AS is_super_over,
        o.value:over::INT                                           AS over_no,       
        d.index + 1                                                 AS ball_seq,      

        d.value:bowler::TEXT                                        AS bowler,
        d.value:batter::TEXT                                        AS batter,
        d.value:non_striker::TEXT                                   AS non_striker,
        COALESCE(GET(m.info:registry.people, d.value:bowler::TEXT)::TEXT,      d.value:bowler::TEXT)      AS bowler_registry_id,
        COALESCE(GET(m.info:registry.people, d.value:batter::TEXT)::TEXT,      d.value:batter::TEXT)      AS batter_registry_id,
        COALESCE(GET(m.info:registry.people, d.value:non_striker::TEXT)::TEXT, d.value:non_striker::TEXT) AS non_striker_registry_id,

        COALESCE(d.value:runs.batter::INT, 0)                       AS runs_batter,
        COALESCE(d.value:runs.extras::INT, 0)                       AS extra_runs,
        COALESCE(d.value:runs.total::INT, 0)                        AS total_runs,
        COALESCE(d.value:runs.non_boundary::BOOLEAN, FALSE)         AS non_boundary,  

        COALESCE(d.value:extras.wides::INT, 0)                      AS wides,
        COALESCE(d.value:extras.noballs::INT, 0)                    AS noballs,
        COALESCE(d.value:extras.byes::INT, 0)                       AS byes,
        COALESCE(d.value:extras.legbyes::INT, 0)                    AS legbyes,
        COALESCE(d.value:extras.penalty::INT, 0)                    AS penalty,

        COALESCE(ARRAY_SIZE(d.value:wickets), 0)                    AS wicket_count,
        d.value:wickets[0].player_out::TEXT                         AS w1_player_out,
        
        COALESCE(GET(m.info:registry.people, d.value:wickets[0].player_out::TEXT)::TEXT,
                 d.value:wickets[0].player_out::TEXT)               AS w1_player_out_registry_id,
        d.value:wickets[0].kind::TEXT                               AS w1_kind,
        d.value:wickets[1].player_out::TEXT                          AS w2_player_out,
        COALESCE(GET(m.info:registry.people, d.value:wickets[1].player_out::TEXT)::TEXT,
                 d.value:wickets[1].player_out::TEXT)                AS w2_player_out_registry_id,
        d.value:wickets[1].kind::TEXT                               AS w2_kind
    FROM cricket.raw.match_raw_vw m,
         LATERAL FLATTEN(INPUT => m.innings) i,
         LATERAL FLATTEN(INPUT => i.value:overs) o,
         LATERAL FLATTEN(INPUT => o.value:deliveries) d
)
SELECT
    b.*,
    
    (wides = 0 AND noballs = 0)                                     AS is_legal_ball,
    (runs_batter = 4 AND NOT non_boundary)                          AS is_four,
    (runs_batter = 6)                                               AS is_six,
    
    IFF(w1_kind IS NOT NULL AND w1_kind NOT IN ('retired hurt', 'retired not out'), 1, 0)
  + IFF(w2_kind IS NOT NULL AND w2_kind NOT IN ('retired hurt', 'retired not out'), 1, 0)
                                                                    AS dismissal_count,
    
    IFF(w1_kind IN ('bowled','caught','lbw','stumped','hit wicket','caught and bowled'), 1, 0)
  + IFF(w2_kind IN ('bowled','caught','lbw','stumped','hit wicket','caught and bowled'), 1, 0)
                                                                    AS bowler_wickets,
    CASE
        WHEN wides   > 0 THEN 'wides'
        WHEN noballs > 0 THEN 'noballs'
        WHEN byes    > 0 THEN 'byes'
        WHEN legbyes > 0 THEN 'legbyes'
        WHEN penalty > 0 THEN 'penalty'
        ELSE 'None'
    END                                                             AS extra_type,
FROM base b;
/* SECTION 5 - CONSUMPTION DIMENSIONS. */
CREATE OR REPLACE TABLE cricket.consumption.date_dim (
    date_id        INT PRIMARY KEY,
    full_dt        DATE NOT NULL,
    day            INT,
    month          INT,
    year           INT,
    quarter        INT,
    dayofweek      INT,          
    dayofyear      INT,
    dayofweekname  VARCHAR(3),
    isweekend      BOOLEAN
);

INSERT INTO cricket.consumption.date_dim
SELECT
    TO_NUMBER(TO_CHAR(dt, 'YYYYMMDD')),
    dt,
    DAY(dt),
    MONTH(dt),
    YEAR(dt),
    QUARTER(dt),
    DAYOFWEEKISO(dt),
    DAYOFYEAR(dt),
    DAYNAME(dt),
    DAYOFWEEKISO(dt) >= 6
FROM (
    SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '1876-01-01'::DATE) AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 60000))
)
WHERE dt <= '2035-12-31';

SELECT COUNT(*) AS total_dates, MIN(full_dt) AS first_date, MAX(full_dt) AS last_date
FROM cricket.consumption.date_dim;


CREATE OR REPLACE TABLE cricket.consumption.team_dim (
    team_id   INT AUTOINCREMENT PRIMARY KEY,
    team_name TEXT NOT NULL,
    gender    TEXT NOT NULL
);

INSERT INTO cricket.consumption.team_dim (team_id, team_name, gender) VALUES (0, 'NA', 'NA');

INSERT INTO cricket.consumption.team_dim (team_name, gender)
SELECT first_team, gender
FROM cricket.clean.match_detail_clean
WHERE first_team IS NOT NULL AND first_team <> 'NA' AND gender <> 'NA'
UNION
SELECT second_team, gender
FROM cricket.clean.match_detail_clean
WHERE second_team IS NOT NULL AND second_team <> 'NA' AND gender <> 'NA'
ORDER BY 1, 2;


CREATE OR REPLACE TABLE cricket.consumption.player_dim (
    player_id           INT AUTOINCREMENT PRIMARY KEY,
    player_registry_id  TEXT NOT NULL,
    player_name         TEXT NOT NULL
);

INSERT INTO cricket.consumption.player_dim (player_id, player_registry_id, player_name)
VALUES (0, 'NA', 'NA');                    

INSERT INTO cricket.consumption.player_dim (player_registry_id, player_name)
SELECT player_registry_id, MAX(player_name)
FROM cricket.clean.player_clean_tb1
GROUP BY player_registry_id
ORDER BY 2;


CREATE OR REPLACE TABLE cricket.consumption.venue_dim (
    venue_id       INT AUTOINCREMENT PRIMARY KEY,
    venue_name     TEXT NOT NULL,
    city           TEXT NOT NULL,
    state          TEXT,
    country        TEXT,
    continent      TEXT,
    end_names      TEXT,
    capacity       NUMBER,
    pitch          TEXT,
    flood_light    BOOLEAN,
    established_dt DATE,
    playing_area   TEXT,
    other_sports   TEXT,
    curator        TEXT,
    latitude       NUMBER(10,6),
    longitude      NUMBER(10,6)
);

INSERT INTO cricket.consumption.venue_dim (venue_id, venue_name, city)
VALUES (0, 'NA', 'NA');

INSERT INTO cricket.consumption.venue_dim (venue_name, city)
SELECT DISTINCT venue, city           
FROM cricket.clean.match_detail_clean
ORDER BY 1, 2;


CREATE OR REPLACE TABLE cricket.consumption.match_type_dim (
    match_type_id INT AUTOINCREMENT PRIMARY KEY,
    match_type    TEXT NOT NULL
);

INSERT INTO cricket.consumption.match_type_dim (match_type_id, match_type)
VALUES (0, 'NA');

INSERT INTO cricket.consumption.match_type_dim (match_type)
SELECT DISTINCT match_type
FROM cricket.clean.match_detail_clean
WHERE match_type IS NOT NULL AND match_type <> 'NA'
ORDER BY 1;


CREATE OR REPLACE TABLE cricket.consumption.referee_dim (
    referee_id   INT AUTOINCREMENT PRIMARY KEY,
    referee_name TEXT NOT NULL,
    referee_type TEXT NOT NULL
);

INSERT INTO cricket.consumption.referee_dim (referee_id, referee_name, referee_type)
VALUES (0, 'Unknown', 'Unknown');

INSERT INTO cricket.consumption.referee_dim (referee_name, referee_type)
SELECT DISTINCT referee_name, 'Match Referee'
FROM cricket.clean.match_detail_clean
WHERE referee_name IS NOT NULL
ORDER BY 1;


/* SECTION 6 - MATCH FACT TABLE. */
CREATE OR REPLACE TABLE cricket.consumption.match_fact (
    match_id            NUMBER PRIMARY KEY,
    match_type_number   INT,
    date_id             INT NOT NULL,
    referee_id          INT NOT NULL,
    team_a_id           INT NOT NULL,
    team_b_id           INT NOT NULL,
    match_type_id       INT NOT NULL,
    venue_id            INT NOT NULL,
    total_overs         NUMBER(4),
    balls_per_over      NUMBER(2),

    overs_completed_by_team_a    NUMBER(4),
    remaining_balls_by_team_a    NUMBER(2),   
    legal_balls_by_team_a        NUMBER(5),
    extra_balls_by_team_a        NUMBER(4),   
    extra_runs_by_team_a         NUMBER(4),
    fours_by_team_a              NUMBER(4),
    sixes_by_team_a              NUMBER(4),
    total_score_by_team_a        NUMBER(5),
    wickets_lost_by_team_a       NUMBER(3),

    overs_completed_by_team_b    NUMBER(4),
    remaining_balls_by_team_b    NUMBER(2),
    legal_balls_by_team_b        NUMBER(5),
    extra_balls_by_team_b        NUMBER(4),
    extra_runs_by_team_b         NUMBER(4),
    fours_by_team_b              NUMBER(4),
    sixes_by_team_b              NUMBER(4),
    total_score_by_team_b        NUMBER(5),
    wickets_lost_by_team_b       NUMBER(3),

    toss_winner_team_id INT NOT NULL,
    toss_decision       TEXT NOT NULL,
    match_result        TEXT NOT NULL,
    winner_team_id      INT NOT NULL,        
    win_by_runs         INT,
    win_by_wickets      INT
);

/* Aggregate all non-super-over innings into one row per match. */
INSERT INTO cricket.consumption.match_fact
WITH innings_summary AS (
    
    SELECT
        match_id,
        batting_team,
        COUNT_IF(is_legal_ball)       AS legal_balls,
        COUNT_IF(NOT is_legal_ball)   AS extra_balls,
        SUM(extra_runs)               AS extra_runs,
        COUNT_IF(is_four)             AS fours,
        COUNT_IF(is_six)              AS sixes,
        SUM(total_runs)               AS total_score,
        SUM(dismissal_count)          AS wickets_lost
    FROM cricket.clean.delivery_clean
    WHERE NOT is_super_over
    GROUP BY match_id, batting_team
)
SELECT
    m.match_id,
    m.match_type_number,
    COALESCE(TO_NUMBER(TO_CHAR(m.event_date, 'YYYYMMDD')), 0)         AS date_id,
    COALESCE(r.referee_id, 0)                                       AS referee_id,
    COALESCE(ta.team_id, 0),
    COALESCE(tb.team_id, 0),
    COALESCE(mt.match_type_id, 0),
    COALESCE(v.venue_id, 0),
    m.total_overs,
    m.balls_per_over,

    
    FLOOR(COALESCE(a.legal_balls, 0) / m.balls_per_over),
    MOD(COALESCE(a.legal_balls, 0), m.balls_per_over),
    COALESCE(a.legal_balls, 0),
    COALESCE(a.extra_balls, 0),
    COALESCE(a.extra_runs, 0),
    COALESCE(a.fours, 0),
    COALESCE(a.sixes, 0),
    COALESCE(a.total_score, 0),
    COALESCE(a.wickets_lost, 0),

    
    FLOOR(COALESCE(b.legal_balls, 0) / m.balls_per_over),
    MOD(COALESCE(b.legal_balls, 0), m.balls_per_over),
    COALESCE(b.legal_balls, 0),
    COALESCE(b.extra_balls, 0),
    COALESCE(b.extra_runs, 0),
    COALESCE(b.fours, 0),
    COALESCE(b.sixes, 0),
    COALESCE(b.total_score, 0),
    COALESCE(b.wickets_lost, 0),

    COALESCE(tw.team_id, 0),
    m.toss_decision,
    m.match_result,
    COALESCE(mw.team_id, 0),
    m.win_by_runs,
    m.win_by_wickets
FROM cricket.clean.match_detail_clean m
LEFT JOIN cricket.consumption.team_dim ta
     ON ta.team_name = m.first_team  AND ta.gender = m.gender
LEFT JOIN cricket.consumption.team_dim tb
     ON tb.team_name = m.second_team AND tb.gender = m.gender
LEFT JOIN cricket.consumption.match_type_dim mt
     ON mt.match_type = m.match_type
LEFT JOIN cricket.consumption.venue_dim v
     ON v.venue_name = m.venue AND v.city = m.city

LEFT JOIN cricket.consumption.team_dim tw
     ON tw.team_name = m.toss_winner AND tw.gender = m.gender
LEFT JOIN cricket.consumption.team_dim mw
     ON mw.team_name = m.winner AND mw.gender = m.gender
LEFT JOIN cricket.consumption.referee_dim r
     ON r.referee_name = m.referee_name
LEFT JOIN innings_summary a
     ON a.match_id = m.match_id AND a.batting_team = m.first_team
LEFT JOIN innings_summary b
     ON b.match_id = m.match_id AND b.batting_team = m.second_team;


/* SECTION 7 - DELIVERY FACT TABLE. */
/* Map teams and players to dimension IDs; retain both wicket slots. */
CREATE OR REPLACE TABLE cricket.consumption.delivery_fact AS
SELECT
    d.match_id,
    d.innings_no,
    d.is_super_over,
    d.over_no,
    d.ball_seq,
    COALESCE(bt.team_id, 0)   AS batting_team_id,
    COALESCE(wt.team_id, 0)   AS bowling_team_id,
    COALESCE(bw.player_id, 0) AS bowler_id,
    COALESCE(ba.player_id, 0) AS batter_id,
    COALESCE(ns.player_id, 0) AS non_striker_id,
    d.runs_batter,
    d.extra_runs,
    d.wides,
    d.noballs,
    d.byes,
    d.legbyes,
    d.penalty,
    d.extra_type,
    d.total_runs,
    
    d.runs_batter + d.wides + d.noballs AS bowler_runs_conceded,
    d.is_legal_ball,
    d.is_four,
    d.is_six,
    d.wicket_count,
    d.dismissal_count,
    d.bowler_wickets,
    COALESCE(po1.player_id, 0) AS player_out_1_id,
    COALESCE(po2.player_id, 0) AS player_out_2_id,
    COALESCE(d.w1_kind, 'None') AS player_out_1_kind,
    COALESCE(d.w2_kind, 'None') AS player_out_2_kind
FROM cricket.clean.delivery_clean d
LEFT JOIN cricket.consumption.team_dim bt
       ON bt.team_name = d.batting_team AND bt.gender = d.gender
LEFT JOIN cricket.consumption.team_dim wt
       ON wt.team_name = d.bowling_team AND wt.gender = d.gender
LEFT JOIN cricket.consumption.player_dim bw ON bw.player_registry_id = d.bowler_registry_id
LEFT JOIN cricket.consumption.player_dim ba ON ba.player_registry_id = d.batter_registry_id
LEFT JOIN cricket.consumption.player_dim ns ON ns.player_registry_id = d.non_striker_registry_id
LEFT JOIN cricket.consumption.player_dim po1
       ON po1.player_registry_id = d.w1_player_out_registry_id
LEFT JOIN cricket.consumption.player_dim po2
       ON po2.player_registry_id = d.w2_player_out_registry_id;


CREATE OR REPLACE TABLE cricket.consumption.match_squad AS
SELECT DISTINCT
    pc.match_id,
    COALESCE(t.team_id, 0) AS team_id,
    p.player_id
FROM cricket.clean.player_clean_tb1 pc
LEFT JOIN cricket.consumption.team_dim t
       ON t.team_name = pc.team_name AND t.gender = pc.gender
JOIN cricket.consumption.player_dim p
       ON p.player_registry_id = pc.player_registry_id;


/* SECTION 8 - REPORTING VIEWS. */
CREATE OR REPLACE VIEW cricket.consumption.vw_match_summary AS
SELECT
    mf.match_id,
    dd.full_dt                         AS match_date,
    mtd.match_type,
    ta.team_name                       AS team_a,
    mf.total_score_by_team_a           AS team_a_runs,
    mf.wickets_lost_by_team_a          AS team_a_wickets,
    mf.overs_completed_by_team_a       AS team_a_completed_overs,
    mf.remaining_balls_by_team_a       AS team_a_remaining_balls,
    tb.team_name                       AS team_b,
    mf.total_score_by_team_b           AS team_b_runs,
    mf.wickets_lost_by_team_b          AS team_b_wickets,
    mf.overs_completed_by_team_b       AS team_b_completed_overs,
    mf.remaining_balls_by_team_b       AS team_b_remaining_balls,
    vd.venue_name,
    vd.city,
    tw.team_name                       AS toss_winner,
    mf.toss_decision,
    mf.match_result,
    winner.team_name                   AS match_winner,
    mf.win_by_runs,
    mf.win_by_wickets,
    rd.referee_name
FROM cricket.consumption.match_fact mf
LEFT JOIN cricket.consumption.date_dim       dd     ON mf.date_id            = dd.date_id
LEFT JOIN cricket.consumption.team_dim       ta     ON mf.team_a_id          = ta.team_id
LEFT JOIN cricket.consumption.team_dim       tb     ON mf.team_b_id          = tb.team_id
LEFT JOIN cricket.consumption.team_dim       tw     ON mf.toss_winner_team_id = tw.team_id
LEFT JOIN cricket.consumption.team_dim       winner ON mf.winner_team_id     = winner.team_id
LEFT JOIN cricket.consumption.venue_dim      vd     ON mf.venue_id           = vd.venue_id
LEFT JOIN cricket.consumption.match_type_dim mtd    ON mf.match_type_id      = mtd.match_type_id
LEFT JOIN cricket.consumption.referee_dim    rd     ON mf.referee_id         = rd.referee_id;


CREATE OR REPLACE VIEW cricket.consumption.vw_team_match_stats AS
SELECT
    mf.match_id,
    td.team_name,
    td.gender,
    'Team A'                           AS innings_side,
    mf.total_score_by_team_a           AS total_runs,
    mf.wickets_lost_by_team_a          AS wickets_lost,
    mf.fours_by_team_a                 AS fours,
    mf.sixes_by_team_a                 AS sixes,
    mf.extra_runs_by_team_a            AS extra_runs,
    mf.overs_completed_by_team_a       AS completed_overs,
    mf.remaining_balls_by_team_a       AS remaining_balls
FROM cricket.consumption.match_fact mf
JOIN cricket.consumption.team_dim td ON mf.team_a_id = td.team_id

UNION ALL

SELECT
    mf.match_id,
    td.team_name,
    td.gender,
    'Team B',
    mf.total_score_by_team_b,
    mf.wickets_lost_by_team_b,
    mf.fours_by_team_b,
    mf.sixes_by_team_b,
    mf.extra_runs_by_team_b,
    mf.overs_completed_by_team_b,
    mf.remaining_balls_by_team_b
FROM cricket.consumption.match_fact mf
JOIN cricket.consumption.team_dim td ON mf.team_b_id = td.team_id;


CREATE OR REPLACE VIEW cricket.consumption.vw_batter_performance AS
SELECT
    df.match_id,
    td.team_name,
    pd.player_name                                     AS batter,
    COUNT_IF(df.wides = 0)                             AS balls_faced,
    SUM(df.runs_batter)                                AS batter_runs,
    COUNT_IF(df.is_four)                               AS fours,
    COUNT_IF(df.is_six)                                AS sixes,
    ROUND(SUM(df.runs_batter) * 100.0
          / NULLIF(COUNT_IF(df.wides = 0), 0), 2)      AS strike_rate,
    COUNT_IF(df.player_out_1_id = df.batter_id)
   + COUNT_IF(df.player_out_2_id = df.batter_id)     AS times_out,
    MAX(df.runs_batter)                                AS highest_runs_on_one_delivery
FROM cricket.consumption.delivery_fact df
JOIN cricket.consumption.team_dim   td ON df.batting_team_id = td.team_id
JOIN cricket.consumption.player_dim pd ON df.batter_id       = pd.player_id
WHERE NOT df.is_super_over
GROUP BY df.match_id, td.team_name, pd.player_name;


CREATE OR REPLACE VIEW cricket.consumption.vw_bowler_performance AS
SELECT
    df.match_id,
    td.team_name                                       AS bowling_team,
    pd.player_name                                     AS bowler,
    COUNT_IF(df.is_legal_ball)                         AS legal_balls,
    CONCAT(FLOOR(COUNT_IF(df.is_legal_ball) / 6), '.',
           MOD(COUNT_IF(df.is_legal_ball), 6))         AS overs_bowled,
    SUM(df.bowler_runs_conceded)                       AS runs_conceded,
    SUM(df.bowler_wickets)                             AS wickets,
    ROUND(SUM(df.bowler_runs_conceded) * 6.0
          / NULLIF(COUNT_IF(df.is_legal_ball), 0), 2)  AS economy
FROM cricket.consumption.delivery_fact df
JOIN cricket.consumption.team_dim   td ON df.bowling_team_id = td.team_id
JOIN cricket.consumption.player_dim pd ON df.bowler_id       = pd.player_id
WHERE NOT df.is_super_over
GROUP BY df.match_id, td.team_name, pd.player_name;


/* SECTION 9 - PIPELINE AUDIT AND DATA QUALITY. */
CREATE OR REPLACE TABLE cricket.consumption.pipeline_audit (
    audit_id       NUMBER AUTOINCREMENT,
    pipeline_name  VARCHAR(100),
    match_count    NUMBER,
    delivery_count NUMBER,
    checked_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO cricket.consumption.pipeline_audit (pipeline_name, match_count, delivery_count)
SELECT
    'CRICKET_ANALYTICS',
    (SELECT COUNT(*) FROM cricket.consumption.match_fact),
    (SELECT COUNT(*) FROM cricket.consumption.delivery_fact);


/* Zero issue_count means a check passed; review non-zero values before release. */
CREATE OR REPLACE TABLE cricket.consumption.dq_check_results AS
SELECT 'raw_files_vs_match_fact_rows' AS check_name,
       (SELECT COUNT(*) FROM cricket.raw.match_raw_vw)
     - (SELECT COUNT(*) FROM cricket.consumption.match_fact) AS issue_count,
       CURRENT_TIMESTAMP() AS checked_at
UNION ALL
SELECT 'duplicate_match_id_in_match_fact',
       COUNT(*) - COUNT(DISTINCT match_id), CURRENT_TIMESTAMP()
FROM cricket.consumption.match_fact
UNION ALL
SELECT 'matches_with_unknown_team',
       COUNT(*), CURRENT_TIMESTAMP()
FROM cricket.consumption.match_fact
WHERE team_a_id = 0 OR team_b_id = 0
UNION ALL
SELECT 'deliveries_with_unknown_player',
       COUNT(*), CURRENT_TIMESTAMP()
FROM cricket.consumption.delivery_fact
WHERE bowler_id = 0 OR batter_id = 0 OR non_striker_id = 0
UNION ALL
SELECT 'delivery_runs_vs_match_fact_score_mismatch',
       COUNT(*), CURRENT_TIMESTAMP()
FROM cricket.consumption.match_fact mf
LEFT JOIN (
    SELECT match_id, SUM(total_runs) AS runs
    FROM cricket.consumption.delivery_fact
    WHERE NOT is_super_over
    GROUP BY match_id
) d ON d.match_id = mf.match_id
WHERE mf.total_score_by_team_a + mf.total_score_by_team_b <> COALESCE(d.runs, 0);

SELECT check_name, issue_count,
       IFF(issue_count = 0, 'PASS', 'CHECK') AS status
FROM cricket.consumption.dq_check_results;



/* SECTION 10 - DAILY AUDIT TASK. */
CREATE OR REPLACE TASK cricket.land.cricket_pipeline_audit_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 6 * * * UTC'      
AS
INSERT INTO cricket.consumption.pipeline_audit (pipeline_name, match_count, delivery_count)
SELECT
    'CRICKET_ANALYTICS',
    (SELECT COUNT(*) FROM cricket.consumption.match_fact),
    (SELECT COUNT(*) FROM cricket.consumption.delivery_fact);


EXECUTE TASK cricket.land.cricket_pipeline_audit_task;

/* Enable the recurring schedule only after verifying a successful manual run. */
-- ALTER TASK cricket.land.cricket_pipeline_audit_task RESUME;


SELECT name, state, error_message, scheduled_time, completed_time
FROM TABLE(
    cricket.INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -2, CURRENT_TIMESTAMP()),
        RESULT_LIMIT => 100
    )
)
WHERE name = 'CRICKET_PIPELINE_AUDIT_TASK'
ORDER BY scheduled_time DESC;


/* SECTION 11 - FINAL VALIDATION AND SAMPLE REPORTS. */
SELECT CURRENT_DATABASE() AS current_database,
       CURRENT_SCHEMA()   AS current_schema,
       CURRENT_WAREHOUSE() AS current_warehouse;


SELECT * FROM cricket.consumption.pipeline_audit ORDER BY checked_at DESC;


SELECT * FROM cricket.consumption.vw_match_summary ORDER BY match_date DESC LIMIT 100;


SELECT team_name, gender, total_runs, wickets_lost
FROM cricket.consumption.vw_team_match_stats
ORDER BY total_runs DESC
LIMIT 10;


SELECT team_name, batter, batter_runs, balls_faced, strike_rate, fours, sixes
FROM cricket.consumption.vw_batter_performance
ORDER BY batter_runs DESC
LIMIT 10;


SELECT bowling_team, bowler, wickets, runs_conceded, overs_bowled, economy
FROM cricket.consumption.vw_bowler_performance
ORDER BY wickets DESC, runs_conceded ASC
LIMIT 10;


SELECT batter, SUM(batter_runs) AS total_runs, SUM(balls_faced) AS balls,
       ROUND(SUM(batter_runs) * 100.0 / NULLIF(SUM(balls_faced), 0), 2) AS strike_rate
FROM cricket.consumption.vw_batter_performance
GROUP BY batter
ORDER BY total_runs DESC
LIMIT 10;


