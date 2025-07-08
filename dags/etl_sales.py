from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # type: ignore
from datetime import datetime, timedelta
import boto3
import os

AWS_CONN_ID = 'aws_default'  
BUCKET_NAME = 'sales-raw'
KEY = '2025/07/08/sales_data.csv'

default_args = {
    'owner': 'Adriano',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform_data():
    print("Executando transformação dos dados (placeholder)...")

with DAG(
    'etl_sales',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_for_file = S3KeySensor(
    task_id="wait_for_sales_file",
    bucket_name="sales-raw",
    bucket_key="2025/07/08/sales_data.csv",
    aws_conn_id="aws_default",
    poke_interval=10,
    timeout=300,
)

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    wait_for_file >> transform_task
