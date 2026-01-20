# Jinja템플릿에서 제공하는 데이터 변수 알아보는 dag
from airflow.sdk import DAG, task
import datetime
import pendulum

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2026, 1, 19, tz="Asia/Seoul"),
    catchup=True
) as dag:
    
    @task(taskid='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)
    
    show templates()
