from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator


def upload_csv_to_s3():
    s3 = S3Hook(aws_conn_id="aws_conn")
    s3.load_file(
        filename="/home/ayush/airflow/data/s3.csv",
        key="raw/sample.csv",
        bucket_name="ayush-pyspark-data-bucket-2026",
        replace=True
    )

default_args = {
    'owner': 'nagras',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_aws_s3',
    start_date=datetime(2026, 1, 26),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name="ayush-airflow-data-bucket-2026",
        aws_conn_id='aws_conn',
        region_name="ap-south-1"
    )
    # upload_raw = S3CreateObjectOperator(
    # task_id="upload_raw_file",
    # s3_bucket="ayush-airflow-data-bucket-2026",
    # s3_key="raw/sample.csv",
    # data="id,name\n1,ayush",
    # aws_conn_id="aws_conn"
    # )
    upload_csv = PythonOperator(
    task_id="upload_csv",
    python_callable=upload_csv_to_s3
    ) 
    create_bucket >> upload_csv
