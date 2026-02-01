from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'nagras',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="mysql_test_dag",
    start_date=datetime(2026, 1, 29),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    test_mysql = MySqlOperator(
        task_id="test_mysql",
        mysql_conn_id="sql_conn",
        sql="SELECT 1;"
    )

    create_table = MySqlOperator(
        task_id="create_users_table",
        mysql_conn_id="sql_conn",
        sql="""
        create table if not exists airflow_users (
            id int AUTO_INCREMENT primary key,
            name varchar(100),
            email varchar(150),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    insert_data = MySqlOperator(
        task_id="insert_user",
        mysql_conn_id="sql_conn",
        sql="""
        insert into airflow_users (name, email) values ('Ayush', 'ayushnagrasuuuu@gmail.com');
        """
    )

    update_data = MySqlOperator(
        task_id="updating_user_data",
        mysql_conn_id = "sql_conn",
        sql="""
            update airflow_users set email='ayushnagras7@gmail.com' where name = 'Ayush';
            """
    )

    # Task order
    test_mysql >> create_table >> insert_data >> update_data


# for testing from inside first 
# docker exec -it airflow-airflow-worker-1 bash
# mysql -h 172.17.0.1 -u airflow -p
# Airflow in Docker
# DB on host
# → Use 172.17.0.1
# → MySQL user '@%'
# → Test from worker container