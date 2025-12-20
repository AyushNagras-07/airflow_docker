from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def ayush_si(age,ti):
    last_name = ti.xcom_pull(task_ids = 'get_name',key="First_name")
    print(f"{last_name} This side, my age is {age}")

def get_name(ti):
    ti.xcom_push(key="First_name",value="Ayush")
    ti.xcom_push(key="Last_name",value="Nagras")
    

with DAG(
    default_args=default_args,
    dag_id='our_python_with_python_operator_v04',
    description = 'Our first dag using python operator',
    start_date = datetime(2025,10,29),
    schedule_interval = '@daily'
) as dag: 
    task1 = PythonOperator(
        task_id='ayush_si',
        python_callable=ayush_si,
        op_kwargs = {'age':20}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable = get_name
    )

    task2 >> task1