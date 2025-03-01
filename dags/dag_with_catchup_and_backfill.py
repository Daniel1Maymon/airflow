from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'daniel',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='data_pipeline_with_catchup',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False  # Prevent retroactive runs
)
def data_pipeline():
    
    @task()
    def extract_data():
        return [1, 2, 3, 4, 5]

    @task()
    def process_data(data):
        return [x * 2 for x in data]

    @task()
    def save_data(processed_data):
        print(f"Saving data: {processed_data}")

    data = extract_data()
    processed_data = process_data(data)
    save_data(processed_data)

data_pipeline()
