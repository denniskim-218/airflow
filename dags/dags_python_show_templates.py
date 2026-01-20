# Jinja템플릿에서 제공하는 데이터 변수 알아보는 dag
# typo 오류 여전하네 이번에 show_templates()에서 '_' 누락
# @task(task_id='~')를 @task(taskid='~')로 작성해서 오류남
from airflow.sdk import DAG, task
import datetime
import pendulum

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2026, 1, 19, tz="Asia/Seoul"),
    catchup=True
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)
    
    show_templates()
