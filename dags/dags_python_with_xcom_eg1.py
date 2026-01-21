# 2026.01.21 - python_with_xcom_eg1
# from, import 선언부 순서 상관없음
# ti.xcom_push(key="result2", value=[1.2.3,4]) -> ti.xcom_push(key="result2", value=[1,2,3,4])

from airflow.sdk import DAG, task
import datetime
import pendulum

with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    #task decorator 이용 - python_xcom_push_task1 선언
    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2", value=[1,2,3])

    #task decorator 이용 - python_xcom_push_task2 선언
    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1,2,3,4])

    #task decorator 이용 - python_xcom_pull_task 선언
    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        # 2025/07/06 - 3.0.0 버전부터 task_ids 값을 주지 않으면 Xcom 을 찾지 못함
        # 버그인지, 의도한 것인지는 확실치 않으나 해결될 때까지 task_ids 값을 리스트로 넣어 결과가 어떻게 나오는지 보는 것으로 대체
        # value1 = ti.xcom_pull(key="result1") 2.0.0 버전때의 value 선언부
        value1 = ti.xcom_pull(key="result1", task_ids=['python_xcom_push_task1','python_xcom_push_task2'])
        value2 = ti.xcom_pull(key="result2", task_ids='python_xcom_push_task1')
        print(value1)
        print(value2)

    xcom_push1() >> xcom_push2() >> xcom_pull()
