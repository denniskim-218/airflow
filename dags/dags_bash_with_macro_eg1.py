# 2026.01.20 - bash_with_macro_eg1
# from, import 선언부 순서 상관없음
# env={'START_DATE':'{{data_inverval_start~~ -> env={'START_DATE':'{{data_interval_start~~


from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # START_DATE: 전월 말일, END_DATE:1일 전
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        env={'START_DATE':'{{data_interval_start.in_timezone("Asia/Seoul") | ds}}',
             'END_DATE':'{{(data_interval_end.in_timezone("Asia/Seoul") - macro.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
        },
        bash_command='echo "START_DATE: $START_DATE" && "END_DATE: $END_DATE"'
    )
    