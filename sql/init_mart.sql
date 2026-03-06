-- init_mart.sql
-- 실행 시점: 데이터 적재(Backfill/수집) 이후 "수동 실행" 권장
-- 내용: mart_* 테이블 + materialized views 생성
BEGIN;

-- ============================================================
-- [1] Mart tables
-- ============================================================

DROP TABLE IF EXISTS public.mart_batter_total;
CREATE TABLE public.mart_batter_total AS
SELECT DISTINCT ON (
    A.season,
    A.season_type,
    A.game_data->>'Name',
    COALESCE(A.game_data->>'Team', A.game_data->>'Tm')
)
    A.season,
    A.season_type,
    SPLIT_PART(A.game_data->>'Lev', '-', 2) as league,
    A.game_data->>'Name' as player_name,
    COALESCE(A.game_data->>'Team', A.game_data->>'Tm') as team_name,

    (B.stats->>'wRC+')::NUMERIC::INT as wrc_plus,
    (B.stats->>'WAR')::NUMERIC as war,
    (B.stats->>'wOBA')::NUMERIC as woba,
    (B.stats->>'HardHit%')::NUMERIC as hard_hit_pct,

    ROUND(COALESCE((A.game_data->>'AVG')::NUMERIC, 0), 3) as avg,
    ROUND(COALESCE((A.game_data->>'OPS')::NUMERIC, 0), 3) as ops,
    COALESCE((A.game_data->>'HR')::NUMERIC, 0)::INT as hr,
    COALESCE((A.game_data->>'RBI')::NUMERIC, 0)::INT as rbi
FROM public.mlb_batting_stats A
LEFT JOIN public.mlb_batting B
    ON A.season = (B.stats->>'Season')::INT
    AND LOWER(REPLACE(REPLACE(A.game_data->>'Name', '.', ''), ' ', ''))
      = LOWER(REPLACE(REPLACE(B.stats->>'Name', '.', ''), ' ', ''))
    AND A.season_type = 'RegularSeason'
ORDER BY A.season, A.season_type, player_name;

CREATE INDEX IF NOT EXISTS idx_batter_total_search
  ON public.mart_batter_total(player_name, season);

DROP TABLE IF EXISTS public.mart_pitcher_total;
CREATE TABLE public.mart_pitcher_total AS
SELECT DISTINCT ON (A.season, A.season_type, A.game_data->>'Name')
    A.season,
    A.season_type,
    A.game_data->>'Name' as player_name,
    COALESCE(A.game_data->>'Team', A.game_data->>'Tm') as team_name,

    (B.stats->>'WAR')::NUMERIC as war,
    (B.stats->>'FIP')::NUMERIC as fip,
    (B.stats->>'ERA-')::NUMERIC::INT as era_minus,

    ROUND(COALESCE((A.game_data->>'ERA')::NUMERIC, 0), 2) as era,
    ROUND(COALESCE((A.game_data->>'WHIP')::NUMERIC, 0), 2) as whip,
    COALESCE((A.game_data->>'W')::NUMERIC, 0)::INT as w,
    COALESCE((A.game_data->>'SO')::NUMERIC, 0)::INT as so
FROM public.mlb_pitching_stats A
LEFT JOIN public.mlb_pitching B
    ON A.season = (B.stats->>'Season')::INT
    AND LOWER(REPLACE(REPLACE(A.game_data->>'Name', '.', ''), ' ', ''))
      = LOWER(REPLACE(REPLACE(B.stats->>'Name', '.', ''), ' ', ''))
    AND A.season_type = 'RegularSeason'
ORDER BY A.season, A.season_type, player_name;

CREATE INDEX IF NOT EXISTS idx_pitcher_total_search
  ON public.mart_pitcher_total(player_name, season);

DROP TABLE IF EXISTS public.mart_batter_daily;
CREATE TABLE public.mart_batter_daily AS
SELECT
  r.game_date,
  r.mlb_id,
  COALESCE(r.season, NULLIF((r.game_data->>'Season')::int, 0)) AS season,
  COALESCE(r.season_type, r.game_data->>'SeasonType', 'RegularSeason') AS season_type,

  r.game_data->>'Name' AS player_name,
  COALESCE(r.game_data->>'Team', r.game_data->>'Tm') AS team_name,

  COALESCE((r.game_data->>'H')::numeric, 0)  AS h,
  COALESCE((r.game_data->>'2B')::numeric, 0) AS doubles,
  COALESCE((r.game_data->>'3B')::numeric, 0) AS triples,
  COALESCE((r.game_data->>'HR')::numeric, 0) AS hr,
  COALESCE((r.game_data->>'RBI')::numeric, 0) AS rbi,
  COALESCE((r.game_data->>'BB')::numeric, 0) AS bb,
  COALESCE((r.game_data->>'SO')::numeric, 0) AS so,
  COALESCE((r.game_data->>'HBP')::numeric, 0) AS hbp,
  COALESCE((r.game_data->>'GDP')::numeric, 0) AS gdp,

  ROUND(COALESCE((r.game_data->>'AVG')::numeric, 0), 3) AS avg,
  ROUND(COALESCE((r.game_data->>'OPS')::numeric, 0), 3) AS ops,

  r.game_data AS game_data,
  r.created_at
FROM public.mlb_batting_range_stats r;

CREATE INDEX IF NOT EXISTS idx_mart_batter_daily_player_date
  ON public.mart_batter_daily (player_name, game_date);

CREATE INDEX IF NOT EXISTS idx_mart_batter_daily_season
  ON public.mart_batter_daily (season, season_type);

CREATE INDEX IF NOT EXISTS idx_mart_batter_daily_gin
  ON public.mart_batter_daily USING GIN (game_data);

DROP TABLE IF EXISTS public.mart_pitcher_daily;
CREATE TABLE public.mart_pitcher_daily AS
SELECT
  r.game_date,
  r.mlb_id,
  COALESCE(r.season, NULLIF((r.game_data->>'Season')::int, 0)) AS season,
  COALESCE(r.season_type, r.game_data->>'SeasonType', 'RegularSeason') AS season_type,

  r.game_data->>'Name' AS player_name,
  COALESCE(r.game_data->>'Team', r.game_data->>'Tm') AS team_name,

  ROUND(COALESCE((r.game_data->>'ERA')::numeric, 0), 2)  AS era,
  ROUND(COALESCE((r.game_data->>'WHIP')::numeric, 0), 2) AS whip,
  COALESCE((r.game_data->>'W')::numeric, 0)::int AS w,
  COALESCE((r.game_data->>'L')::numeric, 0)::int AS l,
  COALESCE((r.game_data->>'SO')::numeric, 0)::int AS so,
  COALESCE((r.game_data->>'BB')::numeric, 0)::int AS bb,
  COALESCE((r.game_data->>'H')::numeric, 0)::int  AS h,
  COALESCE((r.game_data->>'HR')::numeric, 0)::int AS hr,
  COALESCE((r.game_data->>'IP')::numeric, 0)      AS ip,

  r.game_data AS game_data,
  r.created_at
FROM public.mlb_pitching_range_stats r;

CREATE INDEX IF NOT EXISTS idx_mart_pitcher_daily_player_date
  ON public.mart_pitcher_daily (player_name, game_date);

CREATE INDEX IF NOT EXISTS idx_mart_pitcher_daily_season
  ON public.mart_pitcher_daily (season, season_type);

CREATE INDEX IF NOT EXISTS idx_mart_pitcher_daily_gin
  ON public.mart_pitcher_daily USING GIN (game_data);

DROP TABLE IF EXISTS public.mart_team_pitching;
CREATE TABLE public.mart_team_pitching AS
SELECT team_idfg, season, team, game_data, created_at
FROM public.mlb_team_pitching;

CREATE INDEX IF NOT EXISTS idx_mart_team_pitching_season
  ON public.mart_team_pitching (season);
CREATE INDEX IF NOT EXISTS idx_mart_team_pitching_team
  ON public.mart_team_pitching (team);
CREATE INDEX IF NOT EXISTS idx_mart_team_pitching_gin
  ON public.mart_team_pitching USING GIN (game_data);

DROP TABLE IF EXISTS public.mart_team_fielding;
CREATE TABLE public.mart_team_fielding AS
SELECT team_idfg, season, team, game_data, created_at
FROM public.mlb_team_fielding;

CREATE INDEX IF NOT EXISTS idx_mart_team_fielding_season
  ON public.mart_team_fielding (season);
CREATE INDEX IF NOT EXISTS idx_mart_team_fielding_team
  ON public.mart_team_fielding (team);
CREATE INDEX IF NOT EXISTS idx_mart_team_fielding_gin
  ON public.mart_team_fielding USING GIN (game_data);

DROP TABLE IF EXISTS public.mart_team_batting;
CREATE TABLE public.mart_team_batting AS
SELECT team_idfg, season, team, game_data, created_at
FROM public.mlb_team_batting;

CREATE INDEX IF NOT EXISTS idx_mart_team_batting_season
  ON public.mart_team_batting (season);
CREATE INDEX IF NOT EXISTS idx_mart_team_batting_team
  ON public.mart_team_batting (team);
CREATE INDEX IF NOT EXISTS idx_mart_team_batting_gin
  ON public.mart_team_batting USING GIN (game_data);

-- ============================================================
-- [2] Materialized views (dashboard / analytics)
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS public.mart_team_season;
CREATE MATERIALIZED VIEW public.mart_team_season AS
SELECT
  team_idfg,
  season,
  team,
  (game_data->>'W')::int            AS wins,
  (game_data->>'L')::int            AS losses,
  (game_data->>'ERA')::numeric      AS era,
  (game_data->>'FIP')::numeric      AS fip,
  (game_data->>'OPS')::numeric      AS ops,
  created_at
FROM public.mlb_team_pitching;

CREATE INDEX IF NOT EXISTS idx_mart_team_season
  ON public.mart_team_season(team, season);

DROP MATERIALIZED VIEW IF EXISTS public.mart_batting_daily;
CREATE MATERIALIZED VIEW public.mart_batting_daily AS
SELECT
  mlb_id,
  game_date,
  season,
  season_type,
  (game_data->>'OPS')::numeric AS ops,
  (game_data->>'wOBA')::numeric AS woba,
  (game_data->>'AB')::int AS ab
FROM public.mlb_batting_range_stats;

CREATE INDEX IF NOT EXISTS idx_mart_batting_daily
  ON public.mart_batting_daily(mlb_id, game_date);

DROP MATERIALIZED VIEW IF EXISTS public.mart_pitch_mix_daily;
CREATE MATERIALIZED VIEW public.mart_pitch_mix_daily AS
WITH base AS (
  SELECT
    date_trunc('day', game_date)::date AS d,
    season,
    season_type,
    pitcher,
    COALESCE(game_data->>'pitch_type', 'UNK') AS pitch_type
  FROM public.mlb_statcast
)
SELECT
  d,
  season,
  season_type,
  pitcher,
  pitch_type,
  COUNT(*) AS n
FROM base
GROUP BY 1,2,3,4,5;

CREATE INDEX IF NOT EXISTS idx_mart_pitch_mix_daily
  ON public.mart_pitch_mix_daily(pitcher, d);

DROP MATERIALIZED VIEW IF EXISTS public.mart_player_daily;
CREATE MATERIALIZED VIEW public.mart_player_daily AS
WITH bat AS (
  SELECT
    'batting'::text AS player_type,
    COALESCE(NULLIF(m.mlb_id,'')::int, NULLIF(m.game_data->>'mlbID','')::int) AS mlb_id,
    m.game_data->>'Name' AS name,
    m.game_data->>'Tm'   AS team,
    m.game_data->>'Opp'  AS opp,
    to_date(m.game_data->>'Date', 'Mon DD, YYYY') AS game_date,
    m.season,
    m.season_type,

    NULLIF(m.game_data->>'PA','')::int AS pa,
    NULLIF(m.game_data->>'AB','')::int AS ab,
    NULLIF(m.game_data->>'H','')::int  AS h,
    NULLIF(m.game_data->>'HR','')::int AS hr,
    NULLIF(m.game_data->>'RBI','')::int AS rbi,
    NULLIF(m.game_data->>'BB','')::int AS bb,
    NULLIF(m.game_data->>'SO','')::int AS so,

    NULLIF(m.game_data->>'BA','')::numeric  AS ba,
    NULLIF(m.game_data->>'OBP','')::numeric AS obp,
    NULLIF(m.game_data->>'SLG','')::numeric AS slg,
    NULLIF(m.game_data->>'OPS','')::numeric AS ops,

    NULL::numeric AS ip,
    NULL::numeric AS era,
    NULL::numeric AS whip,
    NULL::int     AS bf,
    NULL::int     AS pit,
    NULL::numeric AS str
  FROM public.mlb_batting_range_stats m
),
pit AS (
  SELECT
    'pitching'::text AS player_type,
    COALESCE(NULLIF(m.mlb_id,'')::int, NULLIF(m.game_data->>'mlbID','')::int) AS mlb_id,
    m.game_data->>'Name' AS name,
    m.game_data->>'Tm'   AS team,
    m.game_data->>'Opp'  AS opp,
    to_date(m.game_data->>'Date', 'Mon DD, YYYY') AS game_date,
    m.season,
    m.season_type,

    NULL::int AS pa,
    NULLIF(m.game_data->>'AB','')::int AS ab,
    NULLIF(m.game_data->>'H','')::int  AS h,
    NULLIF(m.game_data->>'HR','')::int AS hr,
    NULL::int AS rbi,
    NULLIF(m.game_data->>'BB','')::int AS bb,
    NULLIF(m.game_data->>'SO','')::int AS so,

    NULL::numeric AS ba,
    NULL::numeric AS obp,
    NULL::numeric AS slg,
    NULL::numeric AS ops,

    NULLIF(m.game_data->>'IP','')::numeric  AS ip,
    NULLIF(m.game_data->>'ERA','')::numeric AS era,
    NULLIF(m.game_data->>'WHIP','')::numeric AS whip,
    NULLIF(m.game_data->>'BF','')::int      AS bf,
    NULLIF(m.game_data->>'Pit','')::int     AS pit,
    NULLIF(m.game_data->>'Str','')::numeric AS str
  FROM public.mlb_pitching_range_stats m
)
SELECT * FROM bat
UNION ALL
SELECT * FROM pit;

CREATE INDEX IF NOT EXISTS idx_mart_player_daily_main
  ON public.mart_player_daily (player_type, season, season_type, team, mlb_id, game_date);

COMMIT;

-- 실행 후 필요 시 수동 refresh
-- REFRESH MATERIALIZED VIEW public.mart_team_season;
-- REFRESH MATERIALIZED VIEW public.mart_batting_daily;
-- REFRESH MATERIALIZED VIEW public.mart_pitch_mix_daily;
-- REFRESH MATERIALIZED VIEW public.mart_player_daily;
