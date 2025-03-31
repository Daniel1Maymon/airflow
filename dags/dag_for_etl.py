from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Adjust system path for imports
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# import your ETL functions
from ETL.extract import run_extract
from ETL.transform import run_transform
from ETL.load import run_load

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_mongo_to_postgres',
    default_args=default_args,
    schedule_interval='0 9 * * *',  # once a day at 09:00
    catchup=False,
    tags=['ETL'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=run_load,
    )

    extract_task >> transform_task >> load_task
