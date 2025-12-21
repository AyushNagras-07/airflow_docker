from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context


default_args = {
    'owner':'nagras',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}
a = [{'id':70001,'name':"Ayush"},{'id':70005,'name':"Siddhesh"}]

def outing():
    import os , csv
    info = get_current_context()
    directory_name = f"/opt/airflow/data/raw/{info.get("dag_run").dag_id}/{info.get('ds')}"
    try:
        os.makedirs(directory_name)
        print(f"Directory '{directory_name}' created successfully.")
    except FileExistsError:
        print(f"Directory '{directory_name}' already exists.")

    with open(f"/opt/airflow/data/raw/{info.get("dag_run").dag_id}/{info.get('ds')}/users.csv","w") as f:
        fn=['id','name']
        writer = csv.DictWriter(f,fieldnames=fn)
        writer.writeheader()
        writer.writerows(a)
        print("Done writing ")

def reading():
    import csv
    from airflow.operators.python import get_current_context

    context = get_current_context()

    dag_id = context["dag"].dag_id
    ds = context["ds"]

    raw_file_path = f"/opt/airflow/data/raw/{dag_id}/{ds}/users.csv"

    transformed_rows = []

    with open(raw_file_path, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            transformed_row = {
                "id": int(row["id"]),
                "name": row["name"],
                "ds": ds
            }
            transformed_rows.append(transformed_row)

    print("Transformed data:")
    for row in transformed_rows:
        print(row)
        
with DAG(
    dag_id='project_1_user',
    start_date = datetime(2025,12,18),
    schedule_interval = '@daily',
    default_args=default_args,
    catchup = True
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command = 'echo starting dag'
    )
    task1=PythonOperator(
        task_id='export_data',
        python_callable=outing
    )
    task2 = PythonOperator(
        task_id='transform_users',
        python_callable=reading
    )
    end = BashOperator(
        task_id='end',
        bash_command = 'echo ending dag'
    )
    start >> task1 >> task2 >> end