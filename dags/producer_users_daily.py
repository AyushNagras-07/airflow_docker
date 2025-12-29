from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner':'nagras',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

a = [{'id':70001,'name':"Ayush"},{'id':70005,'name':"Siddhesh"}]


def outing():
    import os , csv
    info = get_current_context()
    directory_name = f"/opt/airflow/data/raw/users/{info.get('ds')}"
    try:
        os.makedirs(directory_name)
        print(f"Directory '{directory_name}' created successfully.")
    except FileExistsError:
        print(f"Directory '{directory_name}' already exists.")

    with open(f"/opt/airflow/data/raw/users/{info.get('ds')}/users.csv","w") as f:
        fn=['id','name']
        writer = csv.DictWriter(f,fieldnames=fn)
        writer.writeheader()
        writer.writerows(a)
        print("Done writing ")

with DAG(
    dag_id= 'producer_users_daily',
    start_date = datetime(2025,12,20),
    schedule_interval='@daily',
    catchup = True,
    default_args=default_args
) as dag :
    start=BashOperator(
        task_id='start',
        bash_command='echo started'
    )
    task1 = PythonOperator(
        task_id='sending_data',
        python_callable=outing
    )
    end = BashOperator(
        task_id='end',
        bash_command='echo ended'
    )
    start >> task1 >> end