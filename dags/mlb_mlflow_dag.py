from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from common.config import DEFAULT_ARGS, MLB_2025_SEASON

from src.mlflow.model_trainer import Modeltrainer
from common.config import DEFAULT_ARGS

def run_ml_inference(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_db_connection', autocommit=True)

    trainer = Modeltrainer()

    X_train, X_test, y_train, y_test = trainer.prepare_features(pg_hook)

    trainer.train_and_log(X_train, X_test, y_train, y_test, model_type='RF')

    trainer.train_and_log(X_train, X_test, y_train, y_test, model_type='XGB')
    
with DAG(
    dag_id='mlb_mlops_pipeline_v1',
    default_args=DEFAULT_ARGS,
    schedule='@weekly',        # 수동 실행 혹은 필요시 '@weekly'로 변경
    catchup=False,
    max_active_runs=1,
    start_date=MLB_2025_SEASON['start_date'],
    end_date=MLB_2025_SEASON['end_date'],

) as dag:
    inference_task = PythonOperator(
        task_id="ml_inference_task",
        python_callable=run_ml_inference
    )
