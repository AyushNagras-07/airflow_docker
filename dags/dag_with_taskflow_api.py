from airflow.decorators import task,dag
from datetime import datetime , timedelta

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id = 'dag_with_taskflow_v2',
     default_args=default_args,
     start_date = datetime(2025,12,17),
     schedule_interval = '@daily')

def trying():
    @task(multiple_outputs = True)
    def get_name():
        return {
            'first_name':'Ayush',
            'last_name' : 'Nagras'
        }
    @task()
    def get_age():
        return 20
    @task()
    def greet(f_name,l_name,age) :
        print(f"My name is {f_name } {l_name} and my age is {age}")
    
    c_name = get_name()
    age = get_age()
    greet(f_name=c_name['first_name'],l_name = c_name['last_name'],age=age)
greet_dag = trying()