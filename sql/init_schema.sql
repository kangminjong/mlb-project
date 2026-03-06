-- init_schema.sql
-- 실행 시점: 컨테이너 최초 기동/DB 초기화 시 자동 실행 OK
-- 내용: Base tables + Dimensions + (안전한) View/Prediction table 까지만 생성
BEGIN;

-- ============================================================
-- [0] Base tables
-- ============================================================

DROP TABLE IF EXISTS public.mlb_batting_range_stats;
CREATE TABLE public.mlb_batting_range_stats (
    mlb_id      VARCHAR(20),
    game_date   DATE,
    game_data   JSONB,
    season      INTEGER,
    season_type VARCHAR(20),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_batting_range PRIMARY KEY (mlb_id, game_date)
);

DROP TABLE IF EXISTS public.mlb_pitching_range_stats;
CREATE TABLE public.mlb_pitching_range_stats (
    mlb_id      VARCHAR(20),
    game_date   DATE,
    game_data   JSONB,
    season      INTEGER,
    season_type VARCHAR(20),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_pitching_range PRIMARY KEY (mlb_id, game_date)
);

DROP TABLE IF EXISTS public.mlb_batting_stats;
CREATE TABLE public.mlb_batting_stats (
    mlb_id      VARCHAR(20),
    season      INTEGER,
    season_type VARCHAR(20),
    game_data   JSONB,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_batting_stats PRIMARY KEY (mlb_id, season, season_type)
);

DROP TABLE IF EXISTS public.mlb_pitching_stats;
CREATE TABLE public.mlb_pitching_stats (
    mlb_id      VARCHAR(20),
    season      INTEGER,
    season_type VARCHAR(20),
    game_data   JSONB,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_pitching_stats PRIMARY KEY (mlb_id, season, season_type)
);

DROP TABLE IF EXISTS public.mlb_team_batting;
CREATE TABLE public.mlb_team_batting (
    team_idfg   INTEGER     NOT NULL,
    season      INTEGER     NOT NULL,
    team        VARCHAR(50),
    team_code   VARCHAR(10),
    game_data   JSONB       NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_team_batting PRIMARY KEY (team_idfg, season)
);

DROP TABLE IF EXISTS public.mlb_team_pitching;
CREATE TABLE public.mlb_team_pitching (
    team_idfg   INTEGER     NOT NULL,
    season      INTEGER     NOT NULL,
    team        VARCHAR(50),
    team_code   VARCHAR(10),
    game_data   JSONB       NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_team_pitching PRIMARY KEY (team_idfg, season)
);

DROP TABLE IF EXISTS public.mlb_team_fielding;
CREATE TABLE public.mlb_team_fielding (
    team_idfg   INTEGER     NOT NULL,
    season      INTEGER     NOT NULL,
    team        VARCHAR(50),
    team_code   VARCHAR(10),
    game_data   JSONB       NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_team_fielding PRIMARY KEY (team_idfg, season)
);

DROP TABLE IF EXISTS public.mlb_batting;
CREATE TABLE public.mlb_batting (
    player_id   VARCHAR(20) NOT NULL,
    season      INTEGER     NOT NULL,
    team        VARCHAR(50),
    stats       JSONB       NOT NULL,
    created_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_mlb_batting PRIMARY KEY (player_id, season)
);

DROP TABLE IF EXISTS public.mlb_pitching;
CREATE TABLE public.mlb_pitching (
    player_id   VARCHAR(20) NOT NULL,
    season      INTEGER     NOT NULL,
    team        VARCHAR(50),
    stats       JSONB       NOT NULL,
    created_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_mlb_pitching PRIMARY KEY (player_id, season)
);

DROP TABLE IF EXISTS public.mlb_statcast;
CREATE TABLE public.mlb_statcast (
    game_pk         INTEGER,
    at_bat_number   INTEGER,
    pitch_number    INTEGER,
    game_date       TIMESTAMP,
    pitcher         INTEGER,
    batter          INTEGER,
    game_data       JSONB,
    season          INTEGER,
    season_type     VARCHAR(20),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_mlb_statcast PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);

-- ============================================================
-- [1] Dimensions
-- ============================================================

DROP TABLE IF EXISTS public.dim_team_mapping;
CREATE TABLE public.dim_team_mapping (
    korean_name VARCHAR(50),
    team_code   VARCHAR(10)
);

CREATE INDEX IF NOT EXISTS idx_mapping_korean
  ON public.dim_team_mapping(korean_name);

TRUNCATE TABLE public.dim_team_mapping;

INSERT INTO public.dim_team_mapping (korean_name, team_code) VALUES
('다저스', 'LAD'), ('LA 다저스', 'LAD'),
('자이언츠', 'SFG'), ('샌프란시스코', 'SFG'),
('파드리스', 'SDP'), ('샌디에이고', 'SDP'), ('김하성네', 'SDP'),
('로키스', 'COL'), ('콜로라도', 'COL'),
('디백스', 'ARI'), ('애리조나', 'ARI'), ('다이아몬드백스', 'ARI'),
('카디널스', 'STL'), ('세인트루이스', 'STL'),
('컵스', 'CHC'), ('시카고 컵스', 'CHC'),
('브루어스', 'MIL'), ('밀워키', 'MIL'),
('레즈', 'CIN'), ('신시내티', 'CIN'),
('파이리츠', 'PIT'), ('피츠버그', 'PIT'),
('브레이브스', 'ATL'), ('애틀랜타', 'ATL'),
('필리스', 'PHI'), ('필라델피아', 'PHI'),
('메츠', 'NYM'), ('뉴욕 메츠', 'NYM'),
('말린스', 'MIA'), ('마이애미', 'MIA'),
('내셔널스', 'WSN'), ('워싱턴', 'WSN'),
('애스트로스', 'HOU'), ('휴스턴', 'HOU'),
('매리너스', 'SEA'), ('시애틀', 'SEA'),
('레인저스', 'TEX'), ('텍사스', 'TEX'),
('에인절스', 'LAA'), ('LA 에인절스', 'LAA'),
('애슬레틱스', 'ATH'), ('오클랜드', 'ATH'),
('트윈스', 'MIN'), ('미네소타', 'MIN'),
('타이거스', 'DET'), ('디트로이트', 'DET'),
('가디언스', 'CLE'), ('클리블랜드', 'CLE'),
('화이트삭스', 'CHW'), ('시카고 화이트삭스', 'CHW'),
('로열스', 'KCR'), ('캔자스시티', 'KCR'),
('오리올스', 'BAL'), ('볼티모어', 'BAL'),
('레이스', 'TBR'), ('탬파베이', 'TBR'),
('블루제이스', 'TOR'), ('토론토', 'TOR'),
('레드삭스', 'BOS'), ('보스턴', 'BOS'),
('양키스', 'NYY'), ('뉴욕 양키스', 'NYY');

-- ============================================================
-- [2] ML / Feature view & Predictions (데이터 없어도 생성 가능)
-- ============================================================

CREATE OR REPLACE VIEW public.v_ml_ops_final_features AS
SELECT
  r.mlb_id::int AS mlb_id,
  r.game_date::date AS game_date,
  COALESCE(r.season, NULLIF((r.game_data->>'Season')::int, 0)) AS season,
  COALESCE(r.season_type, r.game_data->>'SeasonType', 'RegularSeason') AS season_type,
  r.game_data->>'Name' AS player_name,
  COALESCE(r.game_data->>'Team', r.game_data->>'Tm') AS team_name,

  COALESCE((r.game_data->>'H')::numeric, 0)::int    AS hits,
  COALESCE((r.game_data->>'2B')::numeric, 0)::int   AS doubles,
  COALESCE((r.game_data->>'3B')::numeric, 0)::int   AS triples,
  COALESCE((r.game_data->>'HR')::numeric, 0)::int   AS hr,
  COALESCE((r.game_data->>'RBI')::numeric, 0)::int  AS rbi,
  COALESCE((r.game_data->>'BB')::numeric, 0)::int   AS bb,
  COALESCE((r.game_data->>'SO')::numeric, 0)::int   AS so,
  COALESCE((r.game_data->>'HBP')::numeric, 0)::int  AS hbp,
  COALESCE((r.game_data->>'GDP')::numeric, 0)::int  AS gdp,

  COALESCE((r.game_data->>'OPS')::numeric, 0)       AS target_ops
FROM public.mlb_batting_range_stats r
WHERE r.game_data ? 'Name';

DROP TABLE IF EXISTS public.mlb_ops_predictions;
CREATE TABLE public.mlb_ops_predictions (
    player_id        INTEGER NOT NULL,
    player_name      TEXT    NOT NULL,
    target_year      INTEGER NOT NULL,
    predicted_ops    NUMERIC(6,4) NOT NULL,
    model_name       TEXT    NOT NULL,
    mlflow_run_id    TEXT    NOT NULL,
    prediction_date  DATE    NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_mlb_ops_predictions
      PRIMARY KEY (player_id, target_year, model_name, prediction_date)
);

CREATE INDEX IF NOT EXISTS idx_mlb_ops_predictions_lookup
  ON public.mlb_ops_predictions (player_name, target_year, model_name, prediction_date);

COMMIT;
