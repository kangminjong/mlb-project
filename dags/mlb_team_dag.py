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
    c.collect_team_batting(2025)
    c.collect_team_pitching(2025)
    c.collect_team_fielding(2025)

def insert_team_batting(**kwargs):
    save_dir = "/opt/airflow/data/team_batting_data"
    mlb = MlbDatabase()
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir_path = os.path.join(save_dir, "team_batting.json")
    execution_date =  kwargs["ds"]
    try:
        with open(save_dir_path, 'r', encoding='utf-8') as f1:
            raw_data = json.load(f1)
    except FileNotFoundError:
        print("해당 날짜 경기 없음")
        raise AirflowSkipException
    except json.JSONDecodeError as e:
        print(f"오류메시지:{e}")
        print(f"오류경기날짜:{execution_date}")
        json_decode_alarm(f"json파싱에러:{execution_date}")
        raise 
    mlb.insert_team_batting(raw_data, pg_hook)

def insert_team_pitching(**kwargs):
    save_dir = "/opt/airflow/data/team_pitching_data"
    mlb = MlbDatabase()
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir_path = os.path.join(save_dir, "team_pitching.json")
    execution_date =  kwargs["ds"]
    try:
        with open(save_dir_path, 'r', encoding='utf-8') as f1:
            raw_data = json.load(f1)
    except FileNotFoundError:
        print("해당 날짜 경기 없음")
        raise AirflowSkipException
    except json.JSONDecodeError as e:
        print(f"오류메시지:{e}")
        print(f"오류경기날짜:{execution_date}")
        json_decode_alarm(f"json파싱에러:{execution_date}")
        raise 
    mlb.insert_team_pitching(raw_data, pg_hook)

def insert_team_fielding(**kwargs):
    save_dir = "/opt/airflow/data/team_fielding_data"
    execution_date =  kwargs["ds"]
    mlb = MlbDatabase()
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir_path = os.path.join(save_dir, "team_fielding.json")
    try:
        with open(save_dir_path, 'r', encoding='utf-8') as f1:
            raw_data = json.load(f1)
    except FileNotFoundError:
        print("해당 날짜 경기 없음")
        raise AirflowSkipException
    except json.JSONDecodeError as e:
        print(f"오류메시지:{e}")
        print(f"오류경기날짜:{execution_date}")
        json_decode_alarm(f"json파싱에러:{execution_date}")
        raise 
    mlb.insert_team_fielding(raw_data, pg_hook)

with DAG(
    dag_id='my_team_dag', # Airflow 웹에 뜰 이름
    default_args=DEFAULT_ARGS,
    schedule=None, # 매일 실행
    start_date=MLB_2025_SEASON['start_date'],
    # end_date=MLB_2025_SEASON['end_date'],
    catchup=False, # 과거 데이터 한꺼번에 돌리지 않기 (테스트용)
    
    max_active_runs=1 
) as dag:

    task1 = PythonOperator(
        task_id = "collect_task",
        python_callable=run_my_collector
    )       
    task2 = PythonOperator(
        task_id = "insert_team_batting_task",
        python_callable=insert_team_batting,
        retries=0
    )
    
    task3 = PythonOperator(
        task_id = "insert_team_pitching_task",
        python_callable=insert_team_pitching,
        retries=0
    )

    task4 = PythonOperator(
        task_id = "insert_team_fielding_task",
        python_callable = insert_team_fielding,
        retries=0
    )
    
task1 >> task2 >> task3 >> task4