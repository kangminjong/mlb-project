import json
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
    def insert_statcast(self, raw_data, pg_hook):
        rows = []
        if not raw_data:
            return
        game_date = raw_data[0]['game_date']
        game_date = self.clean_date(game_date)
        season = game_date.year
        
        if game_date < datetime(season, 9, 30):
            season_type = 'RegularSeason'
        else:
            season_type = 'Postseason'
        for data in raw_data:
            if self.missing_pk(data, ["game_pk", "at_bat_number", "pitch_number"]):
                continue
            game_pk = data['game_pk']
            at_bat_number = data['at_bat_number']
            pitch_number = data['pitch_number']
            rows.append((game_pk, at_bat_number, pitch_number, game_date, json.dumps(data), season, season_type))
        if not rows:
            return
        pg_hook.insert_rows(table = "mlb_statcast", rows = rows, target_fields = ["game_pk", "at_bat_number", "pitch_number", "game_date", "game_data", "season", "season_type"], replace=True, replace_index=["game_pk", "at_bat_number", "pitch_number"])
        
    def insert_pitching_stats(self, raw_data, pg_hook, season_type):
        rows = []
        for data in raw_data:
            if self.missing_pk(data, ["mlbID"]):
                continue
            mlbID = data['mlbID']
            rows.append((mlbID, 2025, season_type, json.dumps(data),))
        if not rows:
            return
        pg_hook.insert_rows(table = "mlb_pitching_stats", rows = rows, target_fields = ["mlb_id","season", "season_type","game_data"], replace=True, replace_index=["mlb_id", "season", "season_type"])

    def insert_batting_stats(self, raw_data, pg_hook, season_type):
        rows = []
        for data in raw_data:
            if self.missing_pk(data, ["mlbID"]):
                continue
            mlbID = data['mlbID']
            rows.append((mlbID, 2025, season_type, json.dumps(data)))
        if not rows:
            return
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
            if self.missing_pk(data, ["mlbID"]):
                continue
            mlb_id = data["mlbID"]
            rows.append((mlb_id, date_obj, json.dumps(data), season, season_type))
        if not rows:
            return
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

        for data in raw_data:
            if self.missing_pk(data, ["mlbID"]):
                continue
            mlb_id = data["mlbID"]
            rows.append((
                mlb_id,
                date_obj, 
                json.dumps(data),
                season, 
                season_type
            ))
        if not rows:
            return
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
            if self.missing_pk(data, ["teamIDfg","Season","Team"]):
                continue
            team_idfg = data.get("teamIDfg")
            season = data.get("Season")
            team = data.get("Team")
            rows.append((int(team_idfg), int(season), team, json.dumps(data)))
        if not rows:
            return  
        pg_hook.insert_rows(table = "mlb_team_batting", rows = rows, target_fields= ["team_idfg", "season", "team", "game_data"],  replace=True, replace_index=["team_idfg", "season"])
    
    def insert_team_pitching(self, rawdata, pg_hook):
        rows = []
        for data in rawdata:
            if self.missing_pk(data, ["teamIDfg","Season","Team"]):
                continue
            team_idfg = data.get("teamIDfg")
            season = data.get("Season")
            team = data.get("Team")
            rows.append((int(team_idfg), int(season), team, json.dumps(data)))
        if not rows:
            return
        pg_hook.insert_rows(table = "mlb_team_pitching", rows = rows, target_fields= ["team_idfg", "season", "team", "game_data"],  replace=True, replace_index=["team_idfg", "season"])
        
    def insert_team_fielding(self, rawdata, pg_hook):
        rows = []
        for data in rawdata:
            if self.missing_pk(data, ["teamIDfg","Season","Team"]):
                continue
            team_idfg = data.get("teamIDfg")
            season = data.get("Season")
            team = data.get("Team")
            rows.append((int(team_idfg), int(season), team, json.dumps(data)))
        if not rows:
            return
        pg_hook.insert_rows(table = "mlb_team_fielding", rows = rows, target_fields= ["team_idfg", "season", "team", "game_data"],  replace=True, replace_index=["team_idfg", "season"])
    
    def insert_batting(self, rawdata, pg_hook):
        rows = []
        for data in rawdata:
            if self.missing_pk(data, ["IDfg","Season","Team"]):
                continue
            iDfg = data["IDfg"]
            season = data["Season"]
            team = data ["Team"] 
            rows.append((iDfg, season, team, json.dumps(data),))
        if not rows:
            return
        pg_hook.insert_rows(table = "mlb_batting", rows = rows, target_fields = ["player_id", "season", "team", "stats"], replace=True, replace_index=["player_id", "season", "team"])

    def insert_pitching(self, rawdata, pg_hook):
        rows = []
        for data in rawdata:
            if self.missing_pk(data, ["IDfg","Season","Team"]):
                continue
            iDfg = data["IDfg"]
            season = data["Season"]
            team = data ["Team"] 
            rows.append((iDfg, season, team, json.dumps(data),))
        if not rows:
            return
        pg_hook.insert_rows(table = "mlb_pitching", rows = rows, target_fields = ["player_id", "season", "team", "stats"], replace=True, replace_index=["player_id", "season", "team"])
    
    def clean_date(self, game_date):
        game_date = game_date // 1000
        game_date = datetime.fromtimestamp(game_date) # 유니스 타임스태프 라는 시간이라서 1000을 몫나눗셈으로 나누고 해당 함수를 적용한다
        return game_date
    def missing_pk(self, data, pk_fields):
        for k in pk_fields:
            # 1) 키 자체가 없는 경우
            if k not in data:
                return True

            v = data.get(k)

            # 2) JSON null -> Python None
            if v is None:
                return True

            # 3) 문자열인데 "" 또는 "   " 인 경우
            if isinstance(v, str) and v.strip() == "":
                return True
        return False