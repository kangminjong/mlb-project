import requests
from airflow.models import Variable


def callback(context):
    # context : 지금 dag가 돌아갔을때 모든 상황판
    exception = context.get("exception") # 딕셔너리 객체여서 get
    task_instance = context.get("task_instance") # 딕셔너리 객체여서 get
    # 원래 객체를 추출한거라 task_instance.task_id가 맞지만 exception은 조금 특이해서 접근 안해도됨
    message_content = f" 실패 발생! \n태스크: {task_instance.task_id} \n에러: {exception}" 
    
    url =  Variable.get("DISCORD_WEBHOOK_URL")
    msg = {"content":message_content}
    requests.post(url, json=msg)
    
def json_decode_alarm(message_content):
    
    url =  Variable.get("DISCORD_WEBHOOK_URL")
    msg = {"content":message_content}
    requests.post(url, json=msg)