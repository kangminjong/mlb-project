from pybaseball import (
    statcast,
    pitching_stats_range,
    batting_stats_range,
    team_batting,
    team_pitching,
    team_fielding,
    batting_stats,
    pitching_stats,
)
from datetime import date, timedelta, datetime
import time
import os
import requests


class Collection:
    def __init__(self, base_dir="/opt/airflow/data"):
        self.basedir = base_dir
        os.makedirs(self.basedir, exist_ok=True)

        self.statcast_dir = os.path.join(base_dir, "statcast_data")
        self.pitchingstats_dir = os.path.join(base_dir, "pitching_stats_data")
        self.battingstats_dir = os.path.join(base_dir, "batting_stats_data")
        self.batting_stats_range_dir = os.path.join(base_dir, "batting_stats_range_data")
        self.pitching_stats_range_dir = os.path.join(base_dir, "pitching_stats_range_data")
        self.team_fielding_dir = os.path.join(base_dir, "team_fielding_data")
        self.team_batting_dir = os.path.join(base_dir, "team_batting_data")
        self.team_pitching_dir = os.path.join(base_dir, "team_pitching_data")
        self.batting_dir = os.path.join(base_dir, "batting_data")
        self.pitching_dir = os.path.join(base_dir, "pitching_data")

        os.makedirs(self.statcast_dir, exist_ok=True)
        os.makedirs(self.pitchingstats_dir, exist_ok=True)
        os.makedirs(self.battingstats_dir, exist_ok=True)
        os.makedirs(self.batting_stats_range_dir, exist_ok=True)
        os.makedirs(self.pitching_stats_range_dir, exist_ok=True)
        os.makedirs(self.team_fielding_dir, exist_ok=True)
        os.makedirs(self.team_batting_dir, exist_ok=True)
        os.makedirs(self.team_pitching_dir, exist_ok=True)
        os.makedirs(self.batting_dir, exist_ok=True)
        os.makedirs(self.pitching_dir, exist_ok=True)

    # --- helpers ---
    def _write_json_atomic(self, df, file_path: str) -> None:
        tmp_path = file_path + ".tmp"
        df.to_json(tmp_path, orient="records", indent=4)
        os.replace(tmp_path, file_path)

    def _is_no_data_case(self, e: Exception) -> bool:
        msg = str(e).lower()
        keywords = [
            "no data",
            "empty",
            "not found",
            "does not exist",
            "no table",
            "offseason",
            "no games",
        ]
        return any(k in msg for k in keywords)

    # --- collectors (rule: no-data -> return, else raise) ---
    def collect_statcast(self, game_date):
        file_path = os.path.join(self.statcast_dir, f"{game_date}.json")
        time.sleep(1.5)
        try:
            data = statcast(start_dt=game_date, end_dt=game_date)  # try 안으로
            if data is None or data.empty:
                return
            print(file_path)
            self._write_json_atomic(data, file_path)
        except Exception as e:
            if self._is_no_data_case(e):
                return
            raise

    def collect_pitchingstats(self, start_date_RegularSeason, end_date_RegularSeason, start_date_Postseason, end_date_Postseason):
        file_path_regular = os.path.join(self.pitchingstats_dir, "pitching_stat_RegularSeason.json")
        file_path_post = os.path.join(self.pitchingstats_dir, "pitching_stat_Postseason.json")
        time.sleep(1.5)

        try:
            data_regular = pitching_stats_range(start_dt=start_date_RegularSeason, end_dt=end_date_RegularSeason)
            if data_regular is not None and not data_regular.empty:
                self._write_json_atomic(data_regular, file_path_regular)
        except Exception as e:
            if not self._is_no_data_case(e):
                raise

        try:
            data_post = pitching_stats_range(start_dt=start_date_Postseason, end_dt=end_date_Postseason)
            if data_post is not None and not data_post.empty:
                self._write_json_atomic(data_post, file_path_post)
        except Exception as e:
            if not self._is_no_data_case(e):
                raise

    def collect_battingstats(
        self, start_date_RegularSeason, end_date_RegularSeason, start_date_Postseason, end_date_Postseason
    ):
        file_path_regular = os.path.join(self.battingstats_dir, "batting_stat_RegularSeason.json")
        file_path_post = os.path.join(self.battingstats_dir, "batting_stat_Postseason.json")
        time.sleep(1.5)

        # RegularSeason
        try:
            data_regular = batting_stats_range(start_dt=start_date_RegularSeason, end_dt=end_date_RegularSeason)
            if data_regular is not None and not data_regular.empty:
                self._write_json_atomic(data_regular, file_path_regular)
        except Exception as e:
            if not self._is_no_data_case(e):
                raise

        # Postseason
        try:
            data_post = batting_stats_range(start_dt=start_date_Postseason, end_dt=end_date_Postseason)
            if data_post is not None and not data_post.empty:
                self._write_json_atomic(data_post, file_path_post)
        except Exception as e:
            if not self._is_no_data_case(e):
                raise

    def collect_pitching_stats_range(self, game_date):
        file_path = os.path.join(self.pitching_stats_range_dir, f"{game_date}.json")
        time.sleep(5.0)
        try:
            data = pitching_stats_range(start_dt=str(game_date), end_dt=str(game_date))
            if data is None or data.empty:
                return
            self._write_json_atomic(data, file_path)
        except Exception as e:
            if self._is_no_data_case(e):
                return
            raise

    def collect_batting_stats_range(self, game_date):
        file_path = os.path.join(self.batting_stats_range_dir, f"{game_date}.json")
        time.sleep(5.0)
        try:
            data = batting_stats_range(start_dt=str(game_date), end_dt=str(game_date))
            if data is None or data.empty:
                return
            self._write_json_atomic(data, file_path)
        except Exception as e:
            if self._is_no_data_case(e):
                return
            raise

    def collect_team_batting(self, year):
        file_path = os.path.join(self.team_batting_dir, "team_batting.json")
        time.sleep(1.5)
        try:
            data = team_batting(year)
            if data is None or data.empty:
                return
            self._write_json_atomic(data, file_path)
        except Exception as e:
            if self._is_no_data_case(e):
                return
            raise

    def collect_team_pitching(self, year):
        file_path = os.path.join(self.team_pitching_dir, "team_pitching.json")
        time.sleep(1.5)
        try:
            data = team_pitching(year)
            if data is None or data.empty:
                return
            self._write_json_atomic(data, file_path)
        except Exception as e:
            if self._is_no_data_case(e):
                return
            raise

    def collect_team_fielding(self, year):
        file_path = os.path.join(self.team_fielding_dir, "team_fielding.json")
        time.sleep(1.5)
        try:
            data = team_fielding(year)
            if data is None or data.empty:
                return
            self._write_json_atomic(data, file_path)
        except Exception as e:
            if self._is_no_data_case(e):
                return
            raise

    def collect_batting(self, year):
        file_path = os.path.join(self.batting_dir, "batting.json")
        time.sleep(1.5)
        try:
            data = batting_stats(year)
            if data is None or data.empty:
                return
            self._write_json_atomic(data, file_path)
        except Exception as e:
            if self._is_no_data_case(e):
                return
            raise

    def collect_pitching(self, year):
        file_path = os.path.join(self.pitching_dir, "pitching.json")
        time.sleep(1.5)
        try:
            data = pitching_stats(year)
            if data is None or data.empty:
                return
            self._write_json_atomic(data, file_path)
        except Exception as e:
            if self._is_no_data_case(e):
                return
            raise