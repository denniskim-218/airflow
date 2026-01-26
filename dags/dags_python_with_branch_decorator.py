# 2026.01.23 - branch_python_operator
# from, import 선언부 순서 상관없음

from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_branch_decorator",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # task branch 선언
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random
        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b','task_c']
    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    select_random() >> [task_a, task_b, task_c]
    