from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor  # type: ignore
from datetime import datetime, timedelta

from functions import (
    load_data_from_csv,  # task transform_data
    export_sales_parquet,  # task export_sales_parquet
    load_fact_sales,  # task load_fact_sales
)

AWS_CONN_ID = "aws_default"
BUCKET_NAME = "sales-raw"
KEY = "2025/07/08/sales_data.csv"

default_args = {
    "owner": "Adriano",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_sales",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    #  Esperar o arquivo no S3
    wait_for_file = S3KeySensor(
        task_id="wait_for_sales_file",
        bucket_name=BUCKET_NAME,
        bucket_key=KEY,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=10,
        timeout=300,
    )

    #  Extrair / validar / inserir na tabela sales
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=load_data_from_csv,
    )

    #  Gerar Parquet e enviar para bucket sales‑curated
    export_sales_parquet_task = PythonOperator(
        task_id="export_sales_parquet",
        python_callable=export_sales_parquet,
    )

    #  Ler Parquet, calcular total_price e carregar fact_sales
    load_fact_sales_task = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales,
    )

    # encadeamento
    wait_for_file >> transform_data >> export_sales_parquet_task >> load_fact_sales_task
