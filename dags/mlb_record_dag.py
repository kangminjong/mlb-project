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
    execution_date =  kwargs["ds"]
    c.collect_batting(2025)
    c.collect_pitching(2025)
    
def insert_pitching(**kwargs):
    save_dir = "/opt/airflow/data/pitching_data"
    mlb = MlbDatabase()
    execution_date =  kwargs["ds"]
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir_path = os.path.join(save_dir, "pitching.json")
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
        raise AirflowSkipException
    mlb.insert_pitching(raw_data, pg_hook)

def insert_batting(**kwargs):
    save_dir = "/opt/airflow/data/batting_data"
    mlb = MlbDatabase()
    execution_date =  kwargs["ds"]
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir_path = os.path.join(save_dir, "batting.json")
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
        raise AirflowSkipException
    mlb.insert_batting(raw_data, pg_hook)

with DAG(
    dag_id='my_record_dag', # Airflow 웹에 뜰 이름
    default_args=DEFAULT_ARGS,
    start_date=MLB_2025_SEASON['start_date'],
    end_date=MLB_2025_SEASON['end_date'],
    schedule='@daily', # 매일 실행
    catchup=False, # 과거 데이터 한꺼번에 돌리지 않기 (테스트용)
    max_active_runs=1 
) as dag:

    task1 = PythonOperator(
        task_id = "collect_task",
        python_callable=run_my_collector
    )       
    task2 = PythonOperator(
        task_id = "insert_pitching",
        python_callable=insert_pitching
    )
    
    task3 = PythonOperator(
        task_id = "insert_batting",
        python_callable=insert_batting
    )

task1 >> task2 >> task3