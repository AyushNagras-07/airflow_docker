from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json

default_args = {
    'owner':'nagras',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}
#we use to keep data static predefine
# a = [{'id':70001,'name':"Ayush"},{'id':70005,'name':"Siddhesh"}]
# now we use api method for data 
def api_data():
    import requests
    resp = requests.get("https://jsonplaceholder.typicode.com/users")
    resp.raise_for_status()
    return resp.json()

def upload_json_to_s3(data):
    s3 = S3Hook(aws_conn_id="aws_conn")
    s3.load_string(
        string_data=json.dumps(data),
        key="data/sample.json",
        bucket_name="ayush-consumer-producer",
        replace=True,
    )

def producer_task():
    data = api_data()          
    upload_json_to_s3(data)   

#this was for local now we upload data to s3 bucket 

# def outing():
#     import os , csv
#     info = get_current_context()
#     directory_name = f"/opt/airflow/data/raw/users/{info.get('ds')}"
#     try:
#         os.makedirs(directory_name)
#         print(f"Directory '{directory_name}' created successfully.")
#     except FileExistsError:
#         print(f"Directory '{directory_name}' already exists.")

#     with open(f"/opt/airflow/data/raw/users/{info.get('ds')}/users.csv","w") as f:
#         fn=['id','name']
#         writer = csv.DictWriter(f,fieldnames=fn)
#         writer.writeheader()
#         writer.writerows(a)
#         print("Done writing ")

with DAG(
    dag_id= 'producer_users_daily',
    start_date = datetime(2026,2,5),
    schedule_interval='@daily',
    catchup = True,
    default_args=default_args
) as dag :
    start=BashOperator(
        task_id='start',
        bash_command='echo started'
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo ended'
    )

    producer = PythonOperator(
        task_id="producer",
        python_callable=producer_task
    )
    # old task for local storage
    # task1 = PythonOperator(
    #     task_id='sending_data',
    #     python_callable=outing
    # )
    # start >> task1 >> end
    start >> producer >> end