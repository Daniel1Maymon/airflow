from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner': 'daniel',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow_api',
  default_args=default_args,
  start_date=datetime(2024,1,7),
  schedule_interval='@daily',
  catchup=False)
def data_pipline(): # ETL
    
    # Task 1: Extract data
    @task()
    def extract_data():
        print("Extracting data...")
        return [1, 2, 3, 4, 5]  # Example data


    # Task 2: process data
    @task()
    def process_data(data):
        print(f"Processing data: {data}")
        processed_data = [x * 2 for x in data]  # Example: doubling each number
        return processed_data
    
    # Task 3: Process data (path 2)
    @task()
    def process_data_2(data):
        print(f"Processing data in process_data_2: {data}")
        processed_data_2 = [x + 1 for x in data]  # Example: incrementing each number
        return processed_data_2
    
    # Task 4: Save data
    @task()
    def save_data(processed_data, processed_data_2):
        print(f"Saving data from process_data: {processed_data}")
        print(f"Saving data from process_data_2: {processed_data_2}")
        # Example: Save both outputs
        
    # Define tasks
    data = extract_data()
    processed_data = process_data(data=data)
    processed_data_2 = process_data_2(data=data)
    save_data(processed_data=processed_data, processed_data_2=processed_data_2)

    
    # Explicitly define dependencies
    # data >> processed_data >> save_data
    
data_pipline()