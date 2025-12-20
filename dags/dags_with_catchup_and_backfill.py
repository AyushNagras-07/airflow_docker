from airflow import DAG
from airflow.decorators import dag,task
from datetime import datetime , timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'dag_with_catchup_and_backfill',
    default_args = default_args,
    start_date = datetime(2025,12,11),
    schedule_interval = '@daily',
    catchup = True
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo this is simple command '
    )