from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from src.collectors.MLBDataCollector import Collection
from common.config import DEFAULT_ARGS, MLB_2025_SEASON
from src.databases.mlb_db import MlbDatabase
import os
import json
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from common.callbacks import json_decode_alarm
def run_my_collector(**kwargs):
    execution_date =  kwargs["ds"]
    c = Collection()
    c.collect_pitching_stats_range(execution_date)
    c.collect_batting_stats_range(execution_date)

def insert_pitching_stats_range(**kwargs):
    execution_date =  kwargs["ds"]
    mlb = MlbDatabase()
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir = "/opt/airflow/data/pitching_stats_range_data"
    os.makedirs(save_dir, exist_ok=True) # 해당 폴더가 존재하지 않으면 해당 폴더 생성
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
        raise AirflowSkipException
    mlb.insert_pitching_stats_range(raw_data, pg_hook, execution_date)

def insert_batting_stats_range_data(**kwargs):
    execution_date = kwargs["ds"]
    mlb = MlbDatabase()
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)
    save_dir = "/opt/airflow/data/batting_stats_range_data"
    os.makedirs(save_dir, exist_ok=True)
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
        raise AirflowSkipException
    mlb.insert_batting_stats_range(raw_data, pg_hook, execution_date)

with DAG(   
    dag_id='my_stats_range_dag', # Airflow 웹에 뜰 이름
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
        task_id = "insert_pitching_stats_range_task",
        python_callable=insert_pitching_stats_range
    )
    
    task3 = PythonOperator(
        task_id = "insert_batting_stats_range_task",
        python_callable=insert_batting_stats_range_data
    )
task1 >> task2 >> task3
