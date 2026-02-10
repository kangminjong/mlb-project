from datetime import datetime
from common.callbacks import callback

MLB_2025_SEASON = {
    'start_date': datetime(2025,3,14),
    'end_date': datetime(2025,11,5),
}

DEFAULT_ARGS = {
    'owner': 'minjong',
    'retries': 0,
    'on_failure_callback': callback,
}

BASE_SAVE_DIR = "/opt/airflow/data"