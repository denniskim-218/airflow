# 2026.01.23 - bash_with_variable
# from, import 선언부 순서 상관없음

from airflow.sdk import DAG, Variable
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # 전역변수 호출 파싱할때 실행되므로 권고하지 않음
    var_value = Variable.get("sample_key")

    bash_ver_1 = BashOperator(
        task_id="bash_ver_1",
        bash_command='echo valialbe1: {var_value}'
    )

    # 전역변수는 Jinja템플릿 형태로 이용할 것을 권고하고 있음
    bash_ver_2 = BashOperator(
        task_id='bash_ver_2',
        bash_command='echo variable2: {{var.value.sample_key}}'
    )
