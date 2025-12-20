from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime as dtime

default_args = {
    "owner": "airflow",
    "start_date": dtime(2022, 1, 1)
}

with DAG(
    dag_id="ETL",
    catchup=False,
    default_args=default_args,
    schedule_interval=None,  # Optional: run only when triggered manually
    description="A simple ETL pipeline DAG"
) as dag:

    start = EmptyOperator(task_id="START")
    e = EmptyOperator(task_id="EXTRACTION")
    t = EmptyOperator(task_id="TRANSFORMATION")
    l = EmptyOperator(task_id="LOADING")
    end = EmptyOperator(task_id="END")

    # Set task dependencies
    start >> e >> t >> l >> end
