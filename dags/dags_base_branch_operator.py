# 2026.01.26 - base_branch_operator
# from, import 선언부 순서 상관없음

from airflow.sdk import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_base_branch_operator",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    # 클래스 상속을 위한 선언
    class CustomerBranchOperator(BaseBranchOperator)
    def choose_barch(self, context):
        import random
        print(context)

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b','task_c']
    
    custom_branch_operator = CustomerBranchOperator(task_id='python_barch_task')

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

    custom_branch_operator >> [task_a, task_b, task_c]
    