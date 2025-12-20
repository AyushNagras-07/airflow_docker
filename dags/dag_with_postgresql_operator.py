from datetime import datetime , timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'nagras',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="dag_with_postgres_operator",
    default_args=default_args,
    start_date = datetime(2025,12,16),
    schedule_interval = '@daily'
) as dag:
    task1 = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS sample (
            dt DATE,
            dag_id VARCHAR(20)
            );
            """
    )
    task2 = PostgresOperator(
        task_id='insert_rows',
        postgres_conn_id = 'postgres_localhost',
        sql="""
insert into sample values ('2025-07-07','ayush1')

        """
    )
task1>>task2