# Cài đặt thư viện
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'you',
    'depends_on_past': False, # phụ thuộc lần chạy trước hay không?
    'start_date': datetime(2023, 5, 11), # Ngày bắt đầu chạy
    'retries': 1, # số lần chạy lại nếu fail
    'retry_delay': timedelta(minutes=5) # thời gian chạy lại nếu fail
}

dag = DAG(
    'dim_customer', # dim_customer là tên DAG
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

dbt_run = BashOperator(
    task_id='run_dbt',
    # bash_command='dbt run --profiles-dir /path/to/your/profiles --project-dir /Users/macpro/Documents/github-project/data_warehouse_course --models analytics.dim_customer.yml', 
    bash_command='dbt run --select dim_customer', 
    dag=dag
)

dbt_test = BashOperator(
    task_id='test_dbt',
    # bash_command='dbt run --profiles-dir /path/to/your/profiles --project-dir /Users/macpro/Documents/github-project/data_warehouse_course --models analytics.dim_customer.yml', 
    bash_command='dbt test --select dim_customer', 
    dag=dag
)

dbt_run >> dbt_test
