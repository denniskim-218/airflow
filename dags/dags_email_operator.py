from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.smtp.operators.smtp import EmailOperator
with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        conn_id='conn_smtp_gamil',
        to='dhkim218@naver.com',
        subject='Airflow 스터디메일',
        html_content='Airflow 작업이 완료되었습니다'
    )
