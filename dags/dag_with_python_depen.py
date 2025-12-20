from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'nagras',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def get_sklearn():
    import sklearn
    print(f"version : {sklearn.__version__}")

def get_matplot():
    import matplotlib
    print(f"version : {matplotlib.__version__}")

with DAG(
    default_args=default_args,
    dag_id='python_dependencies',
    start_date=datetime(2025,12,17),
    schedule_interval = '@daily'

) as dag:
    task1 = PythonOperator(
        task_id='sk_learn_version',
        python_callable = get_sklearn
    )

    task2 = PythonOperator(
        task_id='matplot_version',
        python_callable = get_matplot
    )
    task2
