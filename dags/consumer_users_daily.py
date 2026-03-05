from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    "owner": "nagras",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}
def transform_users():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    context = get_current_context()
    ds = context["ds"]

    spark = SparkSession.builder \
        .appName("consumer-transform") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    raw_path = f"s3a://ayush-consumer-producer/data/sample/{ds}.json"
    processed_path = f"s3a://ayush-consumer-producer/data/processed/users/ds={ds}/"

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True)
    ])

    df = spark.read.schema(schema).json(raw_path)

    total_records = df.count()
    print(f"Total records ingested: {total_records}")

    null_count = df.filter(
        col("id").isNull() | col("email").isNull()
    ).count()

    if null_count > 0:
        raise ValueError(f"Data quality check failed. {null_count} rows contain null values.")

    df_processed = df.select(
        col("id"),
        col("name"),
        col("email")
    )

    processed_records = df_processed.count()
    print(f"Total records processed: {processed_records}")

    df_processed.write.mode("overwrite").parquet(processed_path)

    spark.stop()

def load_to_postgres():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    context = get_current_context()
    ds = context["ds"]

    spark = SparkSession.builder \
        .appName("consumer-load") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    processed_path = f"s3a://ayush-consumer-producer/data/processed/users/ds={ds}/"

    df = spark.read.parquet(processed_path)

    rows = df.collect()

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM users_daily WHERE dt = %s", (ds,))

    for r in rows:
        cursor.execute(
            """
            INSERT INTO users_daily (id, name, email, dt)
            VALUES (%s,%s,%s,%s)
            """,
            (r["id"], r["name"], r["email"], ds),
        )

    conn.commit()

    cursor.close()
    conn.close()

    spark.stop()


with DAG(
    dag_id="consumer_users_daily",
    start_date=datetime(2026, 2, 5),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo consumer pipeline started"
    )

    wait_for_producer = ExternalTaskSensor(
        task_id="wait_for_producer_task",
        external_dag_id="producer_users_daily",
        external_task_id="producer",
        poke_interval=60,
        timeout=30000,
        mode="reschedule",
    )

    wait_for_file_s3 = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name="ayush-consumer-producer",
        bucket_key="data/sample/{{ ds }}.json",
        aws_conn_id="aws_conn",
        mode="reschedule",
    )

    transform_task = PythonOperator(
        task_id="transform_with_spark",
        python_callable=transform_users
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo consumer pipeline completed"
    )

    start >> wait_for_producer >> wait_for_file_s3 >> transform_task >> load_task >> end