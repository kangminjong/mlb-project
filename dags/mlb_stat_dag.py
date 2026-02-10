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
    execution_date =  kwargs["ds"]
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir_path_RegularSeason = os.path.join(save_dir, "pitching_stat_RegularSeason.json")
    save_dir_path_Postseason = os.path.join(save_dir, "pitching_stat_Postseason.json")
    print(save_dir_path_RegularSeason)
    try:
        with open(save_dir_path_RegularSeason, 'r', encoding='utf-8') as f1:
            raw_data_RegularSeason = json.load(f1)
        with open(save_dir_path_Postseason, "r", encoding='utf-8') as f2:
            raw_data_Postseason = json.load(f2)
    except FileNotFoundError:
        print("해당 날짜 경기 없음")
        raise AirflowSkipException
    except json.JSONDecodeError as e:
        print(f"오류메시지:{e}")
        print(f"오류경기날짜:{execution_date}")
        json_decode_alarm(f"json파싱에러:{execution_date}")
        raise AirflowSkipException
    mlb.insert_pitching_stats(raw_data_RegularSeason, pg_hook, "RegularSeason")
    mlb.insert_pitching_stats(raw_data_Postseason, pg_hook, "Postseason")

def insert_battingstats(**kwargs):
    save_dir = "/opt/airflow/data/batting_stats_data"
    mlb = MlbDatabase()
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir_path_RegularSeason = os.path.join(save_dir, "batting_stat_RegularSeason.json")
    save_dir_path_Postseason = os.path.join(save_dir, "batting_stat_Postseason.json")
    execution_date = kwargs["ds"]
    execution_date = execution_date[:4]
    try:
        with open(save_dir_path_RegularSeason, 'r', encoding='utf-8') as f1:
            raw_data_RegularSeason = json.load(f1)
        with open(save_dir_path_Postseason, 'r', encoding='utf-8') as f2:
            raw_data_Postseason = json.load(f2)
    except FileNotFoundError:
        print(f"{execution_date} : 해당 날짜 경기 없음")
        raise AirflowSkipException
    except json.JSONDecodeError as e:
        print(f"오류메시지:{e}")
        print(f"오류경기날짜:{execution_date}")
        json_decode_alarm(f"json파싱에러:{execution_date}")
        raise AirflowSkipException
    mlb.insert_batting_stats(raw_data_RegularSeason, pg_hook, "RegularSeason")
    mlb.insert_batting_stats(raw_data_Postseason, pg_hook, "Postseason" )


with DAG(
    dag_id='my_stat_dag', # Airflow 웹에 뜰 이름
    default_args=DEFAULT_ARGS,
    schedule='@daily', # 매일 실행
    catchup=False, # 과거 데이터 한꺼번에 돌리지 않기 (테스트용)
    start_date=MLB_2025_SEASON['start_date'],
    end_date=MLB_2025_SEASON['end_date'],
    max_active_runs=1 
) as dag:

    task1 = PythonOperator(
        task_id = "collect_task",
        python_callable=run_my_collector
    )       
    task2 = PythonOperator(
        task_id = "insert_pitchingstats_task",
        python_callable=insert_pitchingstats
    )
    
    task3 = PythonOperator(
        task_id = "insert_battingstats_task",
        python_callable=insert_battingstats
    )

task1 >> task2 >> task3