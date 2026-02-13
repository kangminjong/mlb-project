import json
import pandas as pd
import os
from datetime import datetime
# conn = psycopg2.connect(
#     host="주소(localhost)",
#     dbname="데이터베이스이름",
#     user="유저이름",
#     password="비밀번호",
#     port="포트번호(보통 5432)"
# )
class MlbDatabase:
    def __init__(self, base_dir = "opt/airflow/data"):
        self.base_dir = base_dir
        
        os.makedirs(self.base_dir, exist_ok=True)
        self.statcast_dir = os.path.join(self.base_dir,"statcast_data")
        self.pitchingstats_dir = os.path.join(self.base_dir,"pitchingstats_data")
        self.battingstats_dir = os.path.join(self.base_dir,"battingstats_data")
        
        os.makedirs(self.statcast_dir, exist_ok=True)
        os.makedirs(self.pitchingstats_dir, exist_ok=True)
        os.makedirs(self.battingstats_dir, exist_ok=True)
    
    def insert_statcast(self, raw_data, pg_hook):
        rows = []
        game_date = raw_data[0]['game_date']
        game_date = self.clean_date(game_date)
        season = game_date.year
        
        if game_date < datetime(season, 9, 30):
            season_type = 'RegularSeason'
        else:
            season_type = 'Postseason'
        for data in raw_data:
            game_pk = data['game_pk']
            at_bat_number = data['at_bat_number']
            pitch_number = data['pitch_number']
            rows.append((game_pk, at_bat_number, pitch_number, game_date, json.dumps(data), season, season_type))
        pg_hook.insert_rows(table = "mlb_statcast", rows = rows, target_fields = ["game_pk", "at_bat_number", "pitch_number", "game_date", "game_data", "season", "season_type"], replace=True, replace_index=["game_pk", "at_bat_number", "pitch_number"])
        
    def insert_pitching_stats(self, raw_data, pg_hook, season_type):
        rows = []
        for data in raw_data:
            mlbID = data['mlbID']
            rows.append((mlbID, 2025, season_type, json.dumps(data),))
        pg_hook.insert_rows(table = "mlb_pitching_stats", rows = rows, target_fields = ["mlb_id","season", "season_type","game_data"], replace=True, replace_index=["mlb_id", "season", "season_type"])

    def insert_batting_stats(self, raw_data, pg_hook, season_type):
        rows = []
        for data in raw_data:
            mlbID = data['mlbID']
            rows.append((mlbID, 2025, season_type, json.dumps(data)))
        pg_hook.insert_rows(table = "mlb_batting_stats", rows = rows, target_fields = ["mlb_id", "season", "season_type", "game_data"], replace=True, replace_index=["mlb_id", "season", "season_type"])
    
    def insert_pitching_stats_range(self, raw_data, pg_hook, game_date):
        rows = []
        date_obj = datetime.strptime(game_date, '%Y-%m-%d')
        season = date_obj.year
        if date_obj < datetime(season, 9, 30):
            season_type = 'RegularSeason'
        else:
            season_type = 'Postseason'
        for data in raw_data:
            mlb_id = data["mlbID"]
            rows.append((mlb_id, date_obj, json.dumps(data), season, season_type))
        pg_hook.insert_rows(table = "mlb_pitching_range_stats", rows = rows, target_fields = ["mlb_id", "game_date", "game_data", "season", "season_type"], replace=True, replace_index=["mlb_id", "game_date"])
        
    def insert_batting_stats_range(self, raw_data, pg_hook, game_date):
        rows = []
        date_obj = datetime.strptime(game_date, "%Y-%m-%d")
        season = date_obj.year
        
        # 2. 시즌 구분 로직
        if date_obj < datetime(season, 9, 30):
            season_type = 'RegularSeason'
        else:
            season_type = 'Postseason'

        # 3. 데이터 가공
        # raw_data 안의 각 선수 데이터를 튜플 형태로 변환
        for data in raw_data:
            mlb_id = data["mlbID"]
            rows.append((
                mlb_id,
                date_obj, 
                json.dumps(data), # JSONB 대응을 위한 직렬화
                season, 
                season_type
            ))

        # 4. 데이터 적재
        # target_fields의 "season_type" 공백 버그를 수정했습니다.
        pg_hook.insert_rows(
            table="mlb_batting_range_stats", 
            rows=rows, 
            target_fields=["mlb_id", "game_date", "game_data", "season", "season_type"],
            replace=True, 
            replace_index=["mlb_id", "game_date"]
        )
        
    
    def insert_team_batting(self, rawdata, pg_hook):
        
        rows = []
        for data in rawdata:
            rows.append((json.dumps(data), ))
        pg_hook.insert_rows(table = "mlb_team_batting", rows = rows, target_fields= ["game_data"])
    
    def insert_team_pitching(self, rawdata, pg_hook):
        rows = []
        for data in rawdata:
            rows.append((json.dumps(data), ))
        pg_hook.insert_rows(table = "mlb_team_pitching", rows = rows, target_fields= ["game_data"])
        
    def insert_team_fielding(self, rawdata, pg_hook):
        rows = []
        for data in rawdata:
            rows.append((json.dumps(data),))
        pg_hook.insert_rows(table = "mlb_team_fielding", rows = rows, target_fields= ["game_data"])
    
    def insert_batting(self, rawdata, pg_hook):
        rows = []
        for data in rawdata:
            iDfg = data["IDfg"]
            season = data["Season"]
            team = data ["Team"] 
            rows.append((iDfg, season, team, json.dumps(data),))
        pg_hook.insert_rows(table = "mlb_batting", rows = rows, target_fields = ["player_id", "season", "team", "stats"], replace=True, replace_index=["player_id", "season", "team"])

    def insert_pitching(self, rawdata, pg_hook):
        rows = []
        for data in rawdata:
            iDfg = data["IDfg"]
            season = data["Season"]
            team = data ["Team"] 
            rows.append((iDfg, season, team, json.dumps(data),))
        pg_hook.insert_rows(table = "mlb_pitching", rows = rows, target_fields = ["player_id", "season", "team", "stats"], replace=True, replace_index=["player_id", "season", "team"])
    
    def clean_date(self, game_date):
        game_date = game_date // 1000
        game_date = datetime.fromtimestamp(game_date) # 유니스 타임스태프 라는 시간이라서 1000을 몫나눗셈으로 나누고 해당 함수를 적용한다
        return game_date