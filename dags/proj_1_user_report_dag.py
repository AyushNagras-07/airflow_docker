from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor



default_args = {
    'owner':'nagras',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}
a = [{'id':70001,'name':"Ayush"},{'id':70005,'name':"Siddhesh"}]

def file_name(exec_date,**kwargs):
    info = get_current_context()
    if exec_date:
        filename = f"{info.get("dag_run").dag_id}/{info.get('ds')}"
    else:
        filename = f"{info.get("dag_run").dag_id}/"


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

def insert_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default', schema='airflow')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    import csv
    context = get_current_context()
    dag_id = context["dag"].dag_id
    ds = context["ds"]
    raw_file_path = f"/opt/airflow/data/raw/{dag_id}/{ds}/users.csv"

    with open(raw_file_path, "r") as f:
        try:
            reader = csv.DictReader(f)
            cursor.execute("delete from users_daily where dt = %s",(ds,))

            for row in reader:
                cursor.execute("insert into users_daily values(%s,%s,%s)",(int(row["id"]),row["name"],ds))

        except Exception as e:
            conn.rollback()
            raise e
        conn.commit()
        cursor.close()
        conn.close()


with DAG(
    dag_id='project_1_user',
    start_date = datetime(2025,12,18),
    schedule_interval = '@daily',
    default_args=default_args,
    catchup = True,
    max_active_runs=1
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command = 'echo starting dag'
    )
    wait_for_file = FileSensor(
        task_id='FileSensor',
        mode="reschedule",
        filepath="/opt/airflow/data/raw/{{ dag.dag_id }}/{{ ds }}/users.csv",
        poke_interval=60,
        timeout=3600
    )
    task1=PythonOperator(
        task_id='export_data',
        python_callable=outing
    )
    task2 = PythonOperator(
        task_id='transform_users',
        python_callable=reading
    )
    task3 = PythonOperator(
        task_id='insert_data_in_database',
        python_callable = insert_data
    )

    end = BashOperator(
        task_id='end',
        bash_command = 'echo ending dag'
    )
    start >> wait_for_file >> task1 >> task2 >> task3 >> end