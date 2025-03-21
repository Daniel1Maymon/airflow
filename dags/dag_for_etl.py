import sys
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
from sqlmodel import create_engine
from pymongo import errors

# Adjusting the system path for ETL imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ETL.ETL_process import extract_data, transform_data, insert_data, connect_to_postgres
from utils.testing import print_hello

# Environment variables
MONGO_URL = os.environ.get("MONGO_URL")
POSTGRES_URL = os.environ.get("POSTGRES_URL")

# MongoDB settings
MONGO_DB = "posts"
MONGO_COLLECTION_EXTRACTED = "etl_extracted"
MONGO_COLLECTION_TRANSFORMED = "etl_transformed"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': 300,  # Retry after 5 minutes
    'catchup': False
}

def extract_task():
    """Extracts data from MongoDB and saves it in 'etl_extracted'."""
    logging.info("Starting MongoDB extraction...")

    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    extracted_collection = db[MONGO_COLLECTION_EXTRACTED]

    # Fetch data using extract_data function
    records = extract_data()

    print("---- records ----")
    print(records)
    print("---- END records ----")

    # Extract _id values from incoming records
    new_ids = [record["_id"] for record in records]

    # Query MongoDB for existing _id values in etl_extracted
    existing_docs = extracted_collection.find(
        {"_id": {"$in": new_ids}}, 
        {"_id": 1})
    existing_ids = {doc["_id"] for doc in existing_docs}

    # Filter out records with existing _id
    filtered_records = [record for record in records if record["_id"] not in existing_ids]

    # Insert only new records
    if filtered_records:
        try:
            extracted_collection.insert_many(filtered_records, ordered=False)
        except errors.BulkWriteError as e:
            # Ignore duplicate key errors (document already exists)
            pass
        client.close()
        logging.info("Extraction complete. Data saved in MongoDB.")

def transform_task():
    print("##### TRANSFORM TASK #####")
    """Transforms extracted data into SQLModel objects and saves in 'etl_transformed'."""
    logging.info("Starting data transformation...")

    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]

    extracted_collection = db[MONGO_COLLECTION_EXTRACTED]
    transformed_collection = db[MONGO_COLLECTION_TRANSFORMED]

    records = list(extracted_collection.find())

    if not records:
        logging.warning("No data to transform.")
        client.close()
        return

    # Extract _id values from extracted records
    new_ids = [record["_id"] for record in records]

    # Find which _id values already exist in etl_transformed
    existing_docs = transformed_collection.find(
        {"_id": {"$in": new_ids}}, 
        {"_id": 1}
    )
    existing_ids = {doc["_id"] for doc in existing_docs}

    # Filter out records that already exist
    filtered_records = [record for record in records if record["_id"] not in existing_ids]

    if not filtered_records:
        logging.info("No new records to transform.")
        client.close()
        return

    # Transform only the filtered records
    transformed_data = transform_data(filtered_records)

    # Insert into etl_transformed with original _id preserved
    try:
        transformed_collection.insert_many([obj.model_dump() for obj in transformed_data], ordered=False)
    except errors.BulkWriteError:
        pass  # Ignore duplicates just in case

    client.close()
    logging.info("Transformation complete. Data saved in MongoDB.")

    print("##### END - TRANSFORM TASK #####")

def load_task():
    """Loads transformed data from MongoDB into PostgreSQL."""
    logging.info("Starting PostgreSQL load...")

    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]

    transformed_collection = db[MONGO_COLLECTION_TRANSFORMED]
    records = list(transformed_collection.find())

    if not records:
        logging.warning("No transformed data to load.")
        client.close()
        return

    # Connect to PostgreSQL
    engine = connect_to_postgres()

    # Insert data into PostgreSQL
    insert_data(engine, records)

    client.close()
    logging.info("Data successfully loaded into PostgreSQL.")

# Define the DAG
with DAG(
    dag_id='etl_mongo_to_postgres',
    default_args=default_args,
    schedule='0 9 * * *',  # Every day at 9:00 AM
    catchup=False,
    tags=['ETL', 'MongoDB', 'PostgreSQL']
) as dag:
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task
    )

    # load = PythonOperator(
    #     task_id='load',
    #     python_callable=load_task
    # )
    extract >> transform
    # extract >> transform >> load
