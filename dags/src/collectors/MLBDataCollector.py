from pybaseball import statcast, pitching_stats_range, batting_stats_range, team_batting, team_pitching, team_fielding, batting_stats, pitching_stats, pybaseball # api 호출
from datetime import date, timedelta, datetime
import time
import os
import requests

# data = statcast(start_dt='2024-10-30', end_dt='2024-10-30') # 클래스 인자값에 경기날짜 넣어서 해당 경기날짜 경기들 전부 가져옴

# print(data.isna().sum()) # 결측치 탐색

# print(data.describe()) # 해당 데이터의 수치 설명

# print(data['pitch_type'].value_counts()) # value_counts : 각 구종의 분포 unique : 각 구종의 이름 
class Collection:
    def __init__(self, base_dir='/opt/airflow/data'):
        self.basedir = base_dir
        os.makedirs(self.basedir, exist_ok=True)
        
        self.statcast_dir = os.path.join(base_dir,"statcast_data")
        self.pitchingstats_dir = os.path.join(base_dir,"pitching_stats_data")
        self.battingstats_dir = os.path.join(base_dir,"batting_stats_data")
        self.batting_stats_range_dir = os.path.join(base_dir,"batting_stats_range_data")
        self.pitching_stats_range_dir = os.path.join(base_dir,"pitching_stats_range_data")
        self.team_fielding_dir = os.path.join(base_dir, "team_fielding_data")
        self.team_batting_dir = os.path.join(base_dir, "team_batting_data")
        self.team_pitching_dir = os.path.join(base_dir, "team_pitching_data")
        self.batting_dir = os.path.join(base_dir, "batting_data")
        self.pitching_dir = os.path.join(base_dir, "pitching_data")
        
        os.makedirs(self.statcast_dir, exist_ok=True)
        os.makedirs(self.pitchingstats_dir, exist_ok=True)
        os.makedirs(self.battingstats_dir, exist_ok=True)
        os.makedirs(self.batting_stats_range_dir,exist_ok=True)
        os.makedirs(self.pitching_stats_range_dir,exist_ok=True)
        os.makedirs(self.team_fielding_dir, exist_ok=True)
        os.makedirs(self.team_batting_dir, exist_ok=True)
        os.makedirs(self.team_pitching_dir, exist_ok=True)
        os.makedirs(self.batting_dir, exist_ok=True)
        os.makedirs(self.pitching_dir, exist_ok=True)
    
    def collect_statcast(self, game_date):
        file_path = os.path.join(self.statcast_dir, f'{game_date}.json')
        time.sleep(1.5)
        data = statcast(start_dt=game_date, end_dt=game_date)
        if not data.empty:
            print(file_path)
            data.to_json(file_path, orient='records', indent=4) # orient= 'records'데이터를 행(Row) 단위의 리스트로 만들어줍니다. (DB 입력용 표준)
    
    def collect_pitchingstats(self,start_date_RegularSeason, end_date_RegularSeason, start_date_Postseason, end_date_Postseason):     
        file_path_RegularSeason = os.path.join(self.pitchingstats_dir,"pitching_stat_RegularSeason.json")
        file_path_Postseason = os.path.join(self.pitchingstats_dir,"pitching_stat_Postseason.json")
        time.sleep(1.5)
        data_RegularSeason = pitching_stats_range(start_dt=start_date_RegularSeason, end_dt=end_date_RegularSeason)
        data_Postseason = pitching_stats_range(start_dt=start_date_Postseason, end_dt=end_date_Postseason)
        if not data_RegularSeason.empty:
            data_RegularSeason.to_json(file_path_RegularSeason, orient="records", indent=4)
        if not data_Postseason.empty:
            data_Postseason.to_json(file_path_Postseason, orient="records", indent=4)
            
    def collect_battingstats(self, start_date_RegularSeason, end_date_RegularSeason, start_date_Postseason, end_date_Postseason):
        file_path_RegularSeason = os.path.join(self.battingstats_dir,"batting_stat_RegularSeason.json")
        file_path_Postseason = os.path.join(self.battingstats_dir,"batting_stat_Postseason.json")
        time.sleep(1.5)
        data_RegularSeason = batting_stats_range(start_dt=start_date_RegularSeason, end_dt=end_date_RegularSeason)
        data_Postseason = batting_stats_range(start_dt=start_date_Postseason, end_dt=end_date_Postseason)
        if not data_RegularSeason.empty:
            data_RegularSeason.to_json(file_path_RegularSeason, orient="records", indent=4)
        if not data_Postseason.empty:
            data_Postseason.to_json(file_path_Postseason, orient="records", indent=4)
    
    def collect_pitching_stats_range(self, game_date):
        file_path = os.path.join(self.pitching_stats_range_dir, f"{game_date}.json")
        time.sleep(5.0)
        try:
            data = pitching_stats_range(start_dt=str(game_date), end_dt=str(game_date))
           
            if not data.empty:
                data.to_json(file_path, orient="records", indent=4)
        except Exception as e:
            print(f"{game_date}: 데이터 테이블이 존재하지 않습니다. (오프시즌/경기없음)")
            print(f"오류메시지:{e}")
    
    def collect_batting_stats_range(self, game_date):
        file_path = os.path.join(self.batting_stats_range_dir, f"{game_date}.json")
        time.sleep(5.0)
        print(game_date)
        try:
            data = batting_stats_range(start_dt=str(game_date), end_dt=str(game_date))
            if not data.empty:
                data.to_json(file_path, orient="records", indent= 4)
        except Exception as e:
            print(f"오류메시지:{e}")
            print(f"{game_date}: 데이터 테이블이 존재하지 않습니다. (오프시즌/경기없음)")

    def collect_team_batting(self, year):
        file_path = os.path.join(self.team_batting_dir,"team_batting.json")
        time.sleep(1.5)
        data = team_batting(year)
        if not data.empty:
            data.to_json(file_path, orient="records", indent=4)
    
    def collect_team_pitching(self, year):
        file_path = os.path.join(self.team_pitching_dir,"team_pitching.json")
        time.sleep(1.5)
        data = team_pitching(year)
        if not data.empty:
            file_path = data.to_json(file_path, orient="records",  indent=4)
    
    def collect_team_fielding(self, year):
        file_path = os.path.join(self.team_fielding_dir,"team_fielding.json")
        time.sleep(1.5)
        data = team_fielding(year)
        if not data.empty:
            file_path = data.to_json(file_path, orient="records", indent=4)
    
    def collect_batting(self, year):
        file_path = os.path.join(self.batting_dir, "batting.json")
        time.sleep(1.5)
        data = batting_stats(year)
        if not data.empty:
            file_path = data.to_json(file_path, orient="records", indent=4)
    
    def collect_pitching(self, year):
        file_path = os.path.join(self.pitching_dir, "pitching.json")
        time.sleep(1.5)
        data = pitching_stats(year)
        if not data.empty:
            file_path = data.to_json(file_path, orient="records", indent=4)