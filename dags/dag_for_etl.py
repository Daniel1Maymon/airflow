
import sys
import os

# Add the absolute path of the ETL folder to the system path (adjusted for Airflow's DAG folder structure)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.testing import print_hello
print("--------")
# print(f"{os.path.abspath(os.path.join(os.path.dirname(__file__), '../ETL'))}")


# from ETL.ETL_process import extract_data

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': 300,  # Retry after 5 minutes
    'catchup': False
}

# Define the DAG
with DAG(
    dag_id='etl_mongo_to_postgres',
    default_args=default_args,
    schedule='0 9 * * *',  # Every day at 9:00 AM
    catchup=False,
    tags=['ETL', 'MongoDB', 'PostgreSQL']
) as dag:

    # def extract_task():
    #     """Extract data from MongoDB."""
    #     return extract_data()

    def print_hello_task():
        """Extract data from MongoDB."""
        return print_hello()
    
    # def transform_task(**context):
    #     """Transform the extracted data."""
    #     extracted_data = context['ti'].xcom_pull(task_ids='extract')
    #     return transform_data(extracted_data)

    # def load_task(**context):
    #     """Load the transformed data into PostgreSQL."""
    #     transformed_data = context['ti'].xcom_pull(task_ids='transform')
    #     engine = connect_to_postgres()
    #     insert_data(engine, transformed_data)

    # Define tasks
    # extract = PythonOperator(
    #     task_id='extract',
    #     python_callable=extract_task,
    #     provide_context=True
    # )

    printing = PythonOperator(
    task_id='print',
    python_callable=print_hello_task,
    provide_context=True
    )
    # transform = PythonOperator(
    #     task_id='transform',
    #     python_callable=transform_task,
    #     provide_context=True
    # )

    # load = PythonOperator(
    #     task_id='load',
    #     python_callable=load_task,
    #     provide_context=True
    # )

    # Task dependencies
    # extract >> transform >> load
    printing
