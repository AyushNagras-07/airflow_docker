from airflow import DAG

from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='second_dag',
    default_args=default_args,
    description='This is my 2nd dag',
    start_date=datetime(2025, 10, 17, 23, 25),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo "Hello world"'
    )

    task2 = BashOperator(
        task_id='Second_task',
        bash_command='echo "this is task 2"'
    )
    task1.set_downstream(task2)