from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'nagras',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def outing():
    print(f"Running daily user report for {{ ds }}")

with DAG(
    dag_id='project_1_user',
    start_date = datetime(2025,12,18),
    schedule_interval = '@daily'
) as dag:
    task1=PythonOperator(
        task_id='prj1',
        python_callable=outing
    )
    task1