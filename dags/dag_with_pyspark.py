from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import get_current_context


import json

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def get_data(**context):
    import requests
    
    url = "https://randomuser.me/api/?results=5"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        users = data["results"]
        json_str = json.dumps(users)
        
        context['task_instance'].xcom_push(key='users_data', value=json_str)
        print(f"Successfully fetched {len(users)} users")
        return json_str
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise


def process_data_with_spark(**context):
    """Process user data using PySpark"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, current_date

    
    # Get data from previous task
    ti = context['task_instance']
    users_json = ti.xcom_pull(task_ids='get_user_data', key='users_data')
    
    if not users_json:
        raise ValueError("No user data found from previous task")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("airflow-pyspark-etl") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Parse JSON and create DataFrame
        users_list = json.loads(users_json)
        
        # Create Spark DataFrame from user data
        df = spark.createDataFrame(users_list)
        
        # Show data
        print(f"Total users: {df.count()}")
        df.printSchema()
        df.show()
        
        # Simple transformation - extract and rename columns
        df_processed = df.select(
        col("id").getItem("value").alias("id"),
        col("email"),
        col("phone"),
        col("gender"),
        col("nat"),
        col("location").getItem("country").alias("country"),
        col("location").getItem("city").alias("city"),
        col("dob").getItem("date").alias("date_of_birth"),
        col("registered").getItem("date").alias("registration_date"),
        current_date().alias("ingestion_date"),
        lit("randomuser_api").alias("source_system")
        )
        
        print("Processed data:")
        df_processed.show()
        
        # Convert to JSON for storage/passing
        rows = df_processed.collect()
        ti.xcom_push(key="processed_data", value=rows)
        
    finally:
        spark.stop()


def save_data(**context):
    """Save processed data (placeholder for DB insert)"""
    ti = context['task_instance']
    rows = ti.xcom_pull(task_ids="process_with_spark", key="processed_data")
    context = get_current_context()
    ds = context["ds"]
    
    if rows:
        print(rows)
        try :
            mysql_hook = MySqlHook(mysql_conn_id="sql_conn")
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM users_daily WHERE ingestion_date = %s",
                (ds,)
            )
            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO users_daily
                    (email, phone, nationality, ingestion_date, source_system,gender)
                    VALUES (%s, %s, %s, %s, %s ,%s)
                    """,
                    (
                        row[1],
                        row[2],
                        row[4],
                        ds,
                        "random_user_api",
                        row[3]
                    )
                )
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            conn.rollback()
            raise

    else:
        print("No processed data found")


with DAG(
    dag_id="pyspark_etl_dag",
    start_date=datetime(2026, 1, 29),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    
    task_get_data = PythonOperator(
        task_id='get_user_data',
        python_callable=get_data,
        provide_context=True
    )

    task_process_spark = PythonOperator(
        task_id='process_with_spark',
        python_callable=process_data_with_spark,
        provide_context=True
    )

    task_save_data = PythonOperator(
        task_id='save_user_data',
        python_callable=save_data,
        provide_context=True
    )
    
    # Set task dependencies
    task_get_data >> task_process_spark >> task_save_data

# for details you can check this https://github.com/AyushNagras-07/airflow_docker/blob/master/etl_pyspark_airflow.png