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
mlb = MlbDatabase()
pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    
def run_my_collector(**kwargs):
    execution_date =  kwargs["ds"]
    c = Collection()
    c.collect_statcast(execution_date)
    
def insert_game(**kwargs):
    save_dir = "/opt/airflow/data/statcast_data"
    os.makedirs(save_dir, exist_ok=True) # 해당 폴더가 존재하지 않으면 해당 폴더 생성
    execution_date = kwargs["ds"]
    save_dir_path = os.path.join(save_dir, f"{execution_date}.json")
    try:
        with open(save_dir_path, 'r', encoding='utf-8') as f1:
            raw_data = json.load(f1)
    except FileNotFoundError:
        print(f"{execution_date} : 해당 날짜 경기 없음")
        raise AirflowSkipException
    except json.JSONDecodeError as e:
        print(f"오류메시지:{e}")
        print(f"오류경기날짜:{execution_date}")
        json_decode_alarm(f"json파싱에러:{execution_date}")
        raise 
    mlb.insert_statcast(raw_data, pg_hook)

with DAG(
    dag_id='my_collection_dag', # Airflow 웹에 뜰 이름
    default_args=DEFAULT_ARGS,
    start_date=MLB_2025_SEASON['start_date'],
    end_date=MLB_2025_SEASON['end_date'],
    schedule='@daily', # 매일 실행
    catchup=True, # 과거 데이터 한꺼번에 돌리지 않기 (테스트용)
    max_active_runs=1 
) as dag:

    task1 = PythonOperator(
        task_id = "collect_task",
        python_callable=run_my_collector
    )       
    task2 = PythonOperator(
        task_id = "insert_task",
        python_callable=insert_game,
        retries=0
    )

task1 >> task2 
