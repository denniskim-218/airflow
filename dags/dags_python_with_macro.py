# 2026.01.21 - python_with_macro
# from, import 선언부 순서 상관없음
# task decorator 사용 시, task도 import 해줘야 함 - from airflow.sdk import DAG, task

from airflow.sdk import DAG, task
import datetime
import pendulum

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    # @task 선언 for using_macro
    @task(task_id='task_using_macro',
        templates_dict={'start_date':'{{(data_interval_end.in_timezone("Asis/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1,day=1)) | ds}}',
                        'end_date':'{{(data_interval_end.in_timezone("Asis/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds}}'
        }
    )

    # def선언 for using_macro
    def get_datetime_macro(**kwargs):
        temlates_dict = kwargs.get('templates_dict') or {}
        if temlates_dict:
            start_date = temlates_dict.get('start_date') or 'start_date 없음'
            end_date = temlates_dict.get('end_date') or 'end_date 없음'
            print(start_date)
            print(end_date)


    # @task 선언 for task_direct_calc
    @task(task_id='task_direct_calc')
    
    # def선언 for task_direct_calc
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone('Asis/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval+end.in_timezone('Asis/Seoul').replace(day=1) + relativedelat(days=-1)
        print(prev_month_day_first.strftime('%y-%m-%d'))
        print(prev_month_day_last.strftime('%y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()    
