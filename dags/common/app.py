import os
import pandas as pd
import streamlit as st
import psycopg2
from dotenv import load_dotenv

load_dotenv()
st.set_page_config(page_title="MLB 선수 성적 대시보드", layout="wide")

DB = dict(
    host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
    port=os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "postgres"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", ""),
)

@st.cache_data(ttl=60)
def q(sql: str, params=None) -> pd.DataFrame:
    with psycopg2.connect(**DB) as conn:
        return pd.read_sql(sql, conn, params=params)

def has_cols(df: pd.DataFrame, cols):
    return [c for c in cols if c in df.columns]

SEASON_TYPE_LABEL = {"R": "정규시즌", "P": "포스트시즌", "S": "시범경기"}

st.title("MLB 선수 성적 대시보드 (시즌 전체 + 월별)")

# -------------------
# Filters
# -------------------
c1, c2, c3, c4, c5 = st.columns(5)

ptype = c1.selectbox("포지션", ["타자(batting)", "투수(pitching)"])
player_type = "batting" if "타자" in ptype else "pitching"

seasons = q("SELECT DISTINCT season FROM mart_player_daily ORDER BY 1")["season"].tolist()
season = c2.selectbox("시즌", seasons)

season_types = q(
    "SELECT DISTINCT season_type FROM mart_player_daily WHERE season=%s ORDER BY 1",
    (season,)
)["season_type"].tolist()
season_type = c3.selectbox(
    "경기종류",
    season_types if season_types else ["R"],
    format_func=lambda x: f"{SEASON_TYPE_LABEL.get(x, x)}({x})"
)

teams = q("""
SELECT DISTINCT team
FROM mart_player_daily
WHERE player_type=%s AND season=%s AND season_type=%s
ORDER BY 1
""", (player_type, season, season_type))["team"].tolist()
team = c4.selectbox("팀", ["ALL(전체)"] + teams)

# 선수 목록(팀=ALL이면 전체)
if team.startswith("ALL"):
    players = q("""
    SELECT DISTINCT mlb_id, name
    FROM mart_player_daily
    WHERE player_type=%s AND season=%s AND season_type=%s
    ORDER BY name
    """, (player_type, season, season_type))
else:
    players = q("""
    SELECT DISTINCT mlb_id, name
    FROM mart_player_daily
    WHERE player_type=%s AND season=%s AND season_type=%s AND team=%s
    ORDER BY name
    """, (player_type, season, season_type, team))

player_label = (players["name"] + " (" + players["mlb_id"].astype(str) + ")").tolist()
pick = c5.selectbox("선수", player_label)
mlb_id = int(pick.split("(")[-1].replace(")", "").strip())

# -------------------
# Load daily
# -------------------
if team.startswith("ALL"):
    daily = q("""
    SELECT *
    FROM mart_player_daily
    WHERE player_type=%s AND season=%s AND season_type=%s AND mlb_id=%s
    ORDER BY game_date
    """, (player_type, season, season_type, mlb_id))
else:
    daily = q("""
    SELECT *
    FROM mart_player_daily
    WHERE player_type=%s AND season=%s AND season_type=%s AND team=%s AND mlb_id=%s
    ORDER BY game_date
    """, (player_type, season, season_type, team, mlb_id))

if daily.empty:
    st.warning("데이터가 없습니다. 팀을 ALL로 바꿔보거나 시즌/경기종류를 바꿔보세요.")
    st.stop()

daily["game_date"] = pd.to_datetime(daily["game_date"])
daily["month"] = daily["game_date"].dt.to_period("M").astype(str)

# -------------------
# 시즌 전체 요약 (원본에 있는 값만)
# -------------------
st.subheader("시즌 전체 요약")

if player_type == "batting":
    # count는 합계, 비율(BA/OBP/SLG)은 '계산하지 않고' 월별/시즌에서 가중치 애매하니 표시 제외하거나,
    # 원본에 시즌 누적이 있으면 그걸 써야 함. 여기선 "카운트"만 KPI로 보여줌.
    for c in ["pa","ab","h","2b","3b","hr","rbi","bb","so","hbp","sf"]:
        if c not in daily.columns:
            daily[c] = 0

    PA = int(daily["pa"].fillna(0).sum())
    AB = int(daily["ab"].fillna(0).sum())
    H  = int(daily["h"].fillna(0).sum())
    HR = int(daily["hr"].fillna(0).sum())
    RBI= int(daily["rbi"].fillna(0).sum())
    BB = int(daily["bb"].fillna(0).sum())
    SO = int(daily["so"].fillna(0).sum())

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("타석(PA)", PA)
    m2.metric("타수(AB)", AB)
    m3.metric("안타(H)", H)
    m4.metric("홈런(HR)", HR)
    m5.metric("타점(RBI)", RBI)
    m6.metric("볼넷(BB)", BB)

else:
    for c in ["ip","h","hr","bb","so","bf","pit"]:
        if c not in daily.columns:
            daily[c] = 0

    IP  = float(daily["ip"].fillna(0).sum())
    H   = int(daily["h"].fillna(0).sum())
    HR  = int(daily["hr"].fillna(0).sum())
    BB  = int(daily["bb"].fillna(0).sum())
    SO  = int(daily["so"].fillna(0).sum())
    PIT = int(daily["pit"].fillna(0).sum())

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("이닝(IP)", round(IP, 1))
    m2.metric("피안타(H)", H)
    m3.metric("피홈런(HR)", HR)
    m4.metric("볼넷(BB)", BB)
    m5.metric("삼진(SO)", SO)
    m6.metric("투구수(PIT)", PIT)

# -------------------
# 월별 성적 요약 (원본 컬럼만 표로)
# -------------------
st.subheader("월별 성적 요약")

if player_type == "batting":
    # 월별 합계는 count 위주로 보여주고,
    # BA/OBP/SLG는 원본에 있으면 월별 평균(참고용)만 표시
    count_cols = has_cols(daily, ["pa","ab","h","2b","3b","hr","rbi","bb","so"])
    rate_cols  = has_cols(daily, ["ba","obp","slg"])  # OPS는 제외

    agg = {c: "sum" for c in count_cols}
    # rate는 참고용 평균(계산 X, 원본 값만)
    for c in rate_cols:
        agg[c] = "mean"

    m = daily.groupby("month", as_index=False).agg(agg).sort_values("month")

    # 반올림 통일
    for c in rate_cols:
        m[c] = pd.to_numeric(m[c], errors="coerce").round(3)

    rename_map = {
        "month":"월",
        "pa":"타석(PA)", "ab":"타수(AB)", "h":"안타(H)", "2b":"2B", "3b":"3B",
        "hr":"홈런(HR)", "rbi":"타점(RBI)", "bb":"볼넷(BB)", "so":"삼진(SO)",
        "ba":"타율(BA)", "obp":"출루율(OBP)", "slg":"장타율(SLG)",
    }
    m = m.rename(columns=rename_map)

    # 보기 좋은 컬럼 순서
    order = ["월"] + [rename_map[c] for c in count_cols] + [rename_map[c] for c in rate_cols]
    order = [c for c in order if c in m.columns]
    st.dataframe(m[order], use_container_width=True)

else:
    count_cols = has_cols(daily, ["ip","h","hr","bb","so","bf","pit"])
    rate_cols  = has_cols(daily, ["era","whip","str"])  # 원본 있으면 참고용

    agg = {c: "sum" for c in count_cols}
    for c in rate_cols:
        agg[c] = "mean"

    m = daily.groupby("month", as_index=False).agg(agg).sort_values("month")

    # 반올림
    if "ip" in m.columns:
        m["ip"] = pd.to_numeric(m["ip"], errors="coerce").round(1)
    for c in rate_cols:
        m[c] = pd.to_numeric(m[c], errors="coerce").round(3)

    rename_map = {
        "month":"월",
        "ip":"이닝(IP)", "h":"피안타(H)", "hr":"피홈런(HR)", "bb":"볼넷(BB)", "so":"삼진(SO)",
        "bf":"BF", "pit":"투구수(PIT)",
        "era":"ERA", "whip":"WHIP", "str":"STR"
    }
    m = m.rename(columns=rename_map)

    order = ["월"] + [rename_map[c] for c in count_cols] + [rename_map[c] for c in rate_cols]
    order = [c for c in order if c in m.columns]
    st.dataframe(m[order], use_container_width=True)