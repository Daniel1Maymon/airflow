import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Adjust system path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ETL.extract import run_extract
# from ETL.transform import run_transform
# from ETL.load import run_load

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': 300,  # 5 minutes
}

with DAG(
    dag_id='etl_mongo_to_postgres',
    default_args=default_args,
    schedule='0 9 * * *',  # Every day at 09:00
    catchup=False,
    tags=['ETL', 'MongoDB', 'PostgreSQL']
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=run_extract
    )

    # transform = PythonOperator(
    #     task_id='transform',
    #     python_callable=run_transform
    # )

    # load = PythonOperator(
    #     task_id='load',
    #     python_callable=run_load
    # )
    extract
    # extract >> transform >> load
