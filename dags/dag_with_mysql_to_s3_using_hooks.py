from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

with DAG(
    dag_id="mysql_test_dag",
    start_date=datetime(2026, 1, 29),
    schedule_interval=None,
    catchup=False
) as dag:

    test_mysql = MySqlOperator(
        task_id="test_mysql",
        mysql_conn_id="sql_conn",
        sql="SELECT 1;"
    )
    test_mysql
