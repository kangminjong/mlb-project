from airflow import DAG
from airflow.operators.python import PythonOperator
from src.collectors.MLBDataCollector import Collection
from common.config import DEFAULT_ARGS, MLB_2025_SEASON
from src.databases.mlb_db import MlbDatabase
import os
import json
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.callbacks import json_decode_alarm

    
def run_my_collector(**kwargs):
    c = Collection()
    c.collect_pitchingstats("2025-03-20", "2025-09-29", "2025-09-30", "2025-11-02")
    c.collect_battingstats("2025-03-20", "2025-09-29", "2025-09-30", "2025-11-02")

def insert_pitchingstats(**kwargs):
    save_dir = "/opt/airflow/data/pitching_stats_data"
    mlb = MlbDatabase()
    pg_hook = PostgresHook(postgres_conn_id="my_db_connection", autocommit=True)

    reg_path = os.path.join(save_dir, "pitching_stat_RegularSeason.json")
    post_path = os.path.join(save_dir, "pitching_stat_Postseason.json")

    # RegularSeason: 없으면 스킵(진짜로 수집이 안 된 거라서)
    try:
        with open(reg_path, "r", encoding="utf-8") as f:
            raw_reg = json.load(f)
    except FileNotFoundError:
        raise AirflowSkipException
    except json.JSONDecodeError as e:
        json_decode_alarm(f"json파싱에러(Regular): {kwargs['ds']} / {e}")
        raise  # ✅ 스킵 말고 실패(재시도는 수집쪽에서 의미있음)

    # Postseason: 없으면 정상(빈 데이터)
    try:
        with open(post_path, "r", encoding="utf-8") as f:
            raw_post = json.load(f)
    except FileNotFoundError:
        raw_post = []
    except json.JSONDecodeError as e:
        json_decode_alarm(f"json파싱에러(Post): {kwargs['ds']} / {e}")
        raise
    mlb.insert_pitching_stats(raw_reg, pg_hook, "RegularSeason")
    if raw_post:
        mlb.insert_pitching_stats(raw_post, pg_hook, "Postseason")
        
def insert_battingstats(**kwargs):
    save_dir = "/opt/airflow/data/batting_stats_data"
    mlb = MlbDatabase()
    pg_hook = PostgresHook(postgres_conn_id="my_db_connection", autocommit=True)

    reg_path = os.path.join(save_dir, "batting_stat_RegularSeason.json")
    post_path = os.path.join(save_dir, "batting_stat_Postseason.json")

    try:
        with open(reg_path, "r", encoding="utf-8") as f:
            raw_reg = json.load(f)
    except FileNotFoundError:
        raise AirflowSkipException
    except json.JSONDecodeError as e:
        json_decode_alarm(f"json파싱에러(Regular): {kwargs['ds']} / {e}")
        raise

    try:
        with open(post_path, "r", encoding="utf-8") as f:
            raw_post = json.load(f)
    except FileNotFoundError:
        raw_post = []
    except json.JSONDecodeError as e:
        json_decode_alarm(f"json파싱에러(Post): {kwargs['ds']} / {e}")
        raise

    mlb.insert_batting_stats(raw_reg, pg_hook, "RegularSeason")
    if raw_post:
        mlb.insert_batting_stats(raw_post, pg_hook, "Postseason")


with DAG(
    dag_id='my_stat_dag', # Airflow 웹에 뜰 이름
    default_args=DEFAULT_ARGS,
    schedule=None, # 매일 실행
    catchup=False, # 과거 데이터 한꺼번에 돌리지 않기 (테스트용)
    start_date=MLB_2025_SEASON['start_date'],
    # end_date=MLB_2025_SEASON['end_date'],
    max_active_runs=1 
) as dag:

    task1 = PythonOperator(
        task_id = "collect_task",
        python_callable=run_my_collector
    )       
    task2 = PythonOperator(
        task_id = "insert_pitchingstats_task",
        python_callable=insert_pitchingstats,
        retries=0
    )
    
    task3 = PythonOperator(
        task_id = "insert_battingstats_task",
        python_callable=insert_battingstats,
        retries=0
    )

task1 >> task2 >> task3