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

def outing():
    info = get_current_context()
    print(f"Logical Date :{info.get("logical_date")}")
    print(f"DS:{info.get('ds')}")
    print(f"Run ID:{info.get("dag_run").run_id}")

with DAG(
    dag_id='project_1_user',
    start_date = datetime(2025,12,7),
    schedule_interval = '@daily',
    default_args=default_args,
    catchup = True
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command = 'echo starting dag'
    )
    task1=PythonOperator(
        task_id='prj1',
        python_callable=outing
    )
    end = BashOperator(
        task_id='end',
        bash_command = 'echo ending dag'
    )
    start >> task1 >> end