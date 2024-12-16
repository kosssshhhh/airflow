from slack_sdk import WebClient
from datetime import datetime
from urllib.parse import quote

class SlackAlert:
    def __init__(self, channel, token):
        self.channel = channel
        self.client = WebClient(token=token)

    def success_msg(self, msg):
        task_instance = msg.get('task_instance')
        if task_instance is None:
            raise ValueError("Message does not contain 'task_instance'")
        
        text = f"""
✨성공했습니다~! ({datetime.today().strftime('%Y-%m-%d')})✨
task id : {task_instance.task_id}
dag id : {task_instance.dag_id}
______________________________
"""
        self.client.chat_postMessage(channel=self.channel, text=text)

    def fail_msg(self, msg):
        task_instance = msg.get('task_instance')
        if task_instance is None:
            raise ValueError("Message does not contain 'task_instance'")
        text = f"""
😭허거걱,, 실패,, ({datetime.today().strftime('%Y-%m-%d')})😭
task id : {task_instance.task_id}
dag id : {task_instance.dag_id}
log url : http://54.180.146.236:8080/dags/{task_instance.dag_id}/grid
______________________________        
"""
        self.client.chat_postMessage(channel=self.channel, text=text)
