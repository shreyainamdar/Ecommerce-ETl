from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Set up paths to access scripts
PROJECT_PATH = "/opt/airflow"
SCRIPTS_PATH = os.path.join(PROJECT_PATH, "scripts")
sys.path.append(SCRIPTS_PATH)

from transform import clean_data
from load_to_bq import load_data_to_bigquery

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL pipeline to clean and load e-commerce data to BigQuery",
) as dag:

    task_clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    task_load_to_bq = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_data_to_bigquery,
    )

    task_clean_data >> task_load_to_bq
