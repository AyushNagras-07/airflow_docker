from datetime import datetime , timedelta
from airflow import DAG

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'nagras',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'dag_with_s3_bucket_minio',
    start_date=datetime(2025,12,17),
    schedule_interval = '@daily',
    default_args=default_args
) as dag:
    task1  = S3KeySensor(
        task_id = 'sensor_minio',
        bucket_name = 'airflow',
        bucket_key = 'data.csv',
        aws_conn_id = 'minio_conn'
    )