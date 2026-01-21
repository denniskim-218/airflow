# 2026.01.21 - python_with_xcom_eg2
# from, import 선언부 순서 상관없음
# retun 'Success' -> return 'Success'

from airflow.sdk import DAG, task
import datetime
import pendulum

with DAG(
    dag_id="python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    #task decorator 이용 - python_xcom_push_by_return 선언
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'

    #task decorator 이용 - python_xcom_pull_1 선언
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값:' + value1)


    #task decorator 이용 - python_xcom_pull_2 선언
    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)

    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()
    