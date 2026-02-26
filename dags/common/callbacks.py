import requests
from airflow.models import Variable


def callback(context):
    ti = context.get("task_instance")
    ex = context.get("exception")

    dag_id = getattr(ti, "dag_id", "unknown")
    task_id = getattr(ti, "task_id", "unknown")
    run_id = context.get("run_id", "unknown")
    ds = context.get("ds", "unknown")
    try_number = getattr(ti, "try_number", "unknown")
    log_url = getattr(ti, "log_url", "unknown")

    message_content = (
        "Airflow Task Failed\n"
        f"- dag: {dag_id}\n"
        f"- task: {task_id}\n"
        f"- ds: {ds}\n"
        f"- run_id: {run_id}\n"
        f"- try: {try_number}\n"
        f"- log: {log_url}\n"
        f"- error: {ex}\n"
    )

    url = Variable.get("DISCORD_WEBHOOK_URL")
    requests.post(url, json={"content": message_content})


def json_decode_alarm(message_content: str):
    url = Variable.get("DISCORD_WEBHOOK_URL")
    requests.post(url, json={"content": f"JSON Decode Error\n{message_content}"})
    
def pk_missing_alarm(message_content: str):
    try:
        url = Variable.get("DISCORD_WEBHOOK_URL")
        requests.post(
            url,
            json={"content": f"PK Missing (skipped rows)\n{message_content}"},
            timeout=3
        )
    except Exception as e:
        print(f"[pk_missing_alarm] failed: {e}")