from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from export_to_parquet import export_sales_to_parquet

default_args = {
    "owner": "Adriano",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 11),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "export_sales_parquet",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    export_task = PythonOperator(
        task_id="export_sales_to_parquet", python_callable=export_sales_to_parquet
    )
