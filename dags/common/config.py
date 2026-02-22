from datetime import datetime,timedelta
from common.callbacks import callback

MLB_2025_SEASON = {
    'start_date': datetime(2025,3,14),
    'end_date': datetime(2025,11,5),
}

DEFAULT_ARGS = {
    "owner": "minjong",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": callback,
}

BASE_SAVE_DIR = "/opt/airflow/data"