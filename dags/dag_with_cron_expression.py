from airflow import DAG
from airflow.decorators import task,dag
from datetime import datetime , timedelta

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args = default_args,
    dag_id = "dag_with_cron",
    start_date= datetime(2025,12,1),
    schedule_interval = '@daily' # you can use '0 0 * * *' instead  of  @daily this is cron expression you can visit crontab.guru to get the cron expression for ceratin time period 
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = "echo hi ayush here "
    )
    task1