from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner':'nagras',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def transform_users():
    from pyspark.sql import SparkSession 
    from pyspark.conf import SparkConf
    spark = SparkSession.builder \
        .appName("consumer-transform") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ) \
        .getOrCreate()    
    ds = get_current_context()["ds"]
    
    raw_path = f"s3a://ayush-consumer-producer/data/sample/{ds}.json"
    processed_path = f"s3a://ayush-consumer-producer/data/processed/users/ds={ds}/"

    df = spark.read.json(raw_path)

    df_processed = df.select(
        "id",
        "name",
        "email"
    )

    df_processed.write.mode("overwrite").parquet(processed_path)

    spark.stop()


# def reading():
#     import csv

#     context = get_current_context()

#     ds = context["ds"]

#     raw_file_path = f"/opt/airflow/data/raw/users/{ds}/users.csv"

#     transformed_rows = []

#     with open(raw_file_path, "r") as f:
#         reader = csv.DictReader(f)

#         for row in reader:
#             transformed_row = {
#                 "id": int(row["id"]),
#                 "name": row["name"],
#                 "ds": ds
#             }
#             transformed_rows.append(transformed_row)

#     print("Transformed data:")
#     for row in transformed_rows:
#         print(row)

def insert_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default', schema='airflow')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    import csv
    context = get_current_context()
    ds = context["ds"]
    raw_file_path = f"/opt/airflow/data/raw/users/{ds}/users.csv"

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
    dag_id='consumer_user_daily',
    start_date = datetime(2026,2,5),
    schedule_interval = '@daily',
    default_args=default_args,
    catchup = False,
    max_active_runs=1
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command = 'echo starting dag'
    )
    first_task = ExternalTaskSensor(
        task_id='wait_for_producer_task',
        external_dag_id='producer_users_daily',
        external_task_id='producer',
        mode = "reschedule",
        timeout=30000 ,
        poke_interval=60,  
    )
    # wait_for_file = FileSensor(
    #     task_id='FileSensor',
    #     mode="reschedule",
    #     filepath="/opt/airflow/data/raw/users/{{ ds }}/users.csv",
    #     poke_interval=60,
    #     timeout=3600
    #     )
    wait_for_file_s3 = S3KeySensor(
        task_id='wait_for_s3_file',
        bucket_name='ayush-consumer-producer',
        bucket_key="data/sample/{{ ds }}.json",
        aws_conn_id='aws_conn',
        mode='reschedule'
    )
    transform_with_spark = PythonOperator(
        task_id ='Loading_and_transforming',
        python_callable=transform_users
    )
    # task2 = PythonOperator(
    #     task_id='transform_users',
    #     python_callable=reading
    # )
    # task3 = PythonOperator(
    #     task_id='insert_data_in_database',
    #     python_callable = insert_data
    # )
    end = BashOperator(
        task_id='end',
        bash_command = 'echo ending dag'
    )
    start >> wait_for_file_s3 >> transform_with_spark >> end 
    #start >> first_task >> wait_for_file_s3 >> transform_with_spark >> end 
    # start >> wait_for_file >> task1 >> task2 >> task3 >> end
    