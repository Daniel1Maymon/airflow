'''
MongoDB --> List of dicts --> SQL Model Objects --> PostgreSQL Table

Pseudocode (with ETL stages)
1. Connect to MongoDB and fetch data.       # Extract - Retrieve data from MongoDB and store it as a list of dictionaries.

2. Process the fetched data into SQL Model objects.  # Transform - Filter and convert each dictionary into an SQL Model object.

3. Connect to PostgreSQL and create a table. # Load - Connect to PostgreSQL and create a table based on the SQL Model.

4. Insert processed data into PostgreSQL.    # Load - Insert all SQL Model objects into the PostgreSQL table.

'''
from datetime import time
import logging, time, os, sys
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlmodel import SQLModel, Session
from sqlalchemy.dialects.postgresql import insert
import pandas as pd


# Load the .env file
load_dotenv()

# Add the path to the project directory
sys.path.append(os.getcwd())

MONGO_URL = os.environ.get("MONGO_URL")
POSTGRES_URL = os.environ.get("POSTGRES_URL")

from utils.regex_extractor import extract_rental_info
from ETL.models.Post import Post


def extract_data(mongo_url: str=MONGO_URL, limit: int = 20) -> list:
    """Extract data from MongoDB and return a list of dictionaries."""
    try:
        # Connect to MongoDB
        logging.info("Connecting to MongoDB.")
        client = MongoClient(host=MONGO_URL)
        db = client["posts"]
        collection = db["collection"]
        logging.info("Connected to MongoDB successfully.")

        # Fetch data (limit to 20 records by default)
        logging.debug(f"Fetching data from MongoDB with limit={limit}.")
        data = list(collection.find().limit(limit))
        logging.info(f"Fetched {len(data)} documents from MongoDB.")
        
        return data

    except Exception as e:
        logging.error(f"Error during MongoDB extraction: {e}")
        raise

    finally:
        client.close()
        logging.info("MongoDB connection closed.")

def transform_data(data: list) -> list:
    import pandas as pd

    # Convert list of dictionaries into a DataFrame
    df = pd.DataFrame(data)

    # Extract 'price', 'rooms', and 'size' using Regex and store in separate columns
    df[['price', 'rooms', 'size']] = df['content'].apply(lambda x: pd.Series(extract_rental_info(x)))

    # Keep only the relevant columns
    df = df[['content', 'link', 'rooms', 'size', 'price']]  # Added 'link'

    # Convert each row of the DataFrame into a SQLModel object
    processed_data = []

    for i, row in df.iterrows():
        row_data = row.to_dict()
        row_data["mongo_id"] = str(data[i]["_id"])  # Store MongoDB _id in mongo_id field

        # Replace NaN values with None
        for key in ['rooms', 'size', 'price']:
            row_data[key] = None if pd.isna(row_data.get(key)) else row_data.get(key)

        # Create a Post object from the row data
        processed_data.append(Post(**row_data))

    return processed_data


def insert_data(engine, data: list):
    """Load - Insert processed SQLModel objects into PostgreSQL using ON CONFLICT DO NOTHING."""
    logging.info(f"Attempting to insert {len(data)} records into PostgreSQL.")  # Log the number of records to insert

    try:
        create_table()  # Ensure the table exists before inserting data

        with Session(engine) as session:  # Open a database session
            # Prepare bulk insert statement using SQLAlchemy's insert function
            stmt = insert(data[0].__class__).values([obj.model_dump() for obj in data])  # Convert each object to a dictionary
            
            # Use ON CONFLICT DO NOTHING to avoid duplicate inserts based on mongo_id
            stmt = stmt.on_conflict_do_nothing(index_elements=["mongo_id"])

            result = session.exec(stmt)  # Execute the statement and capture the result
            session.commit()  # Commit the transaction

            # Calculate the number of inserted rows (rowcount = number of rows affected)
            inserted_count = result.rowcount if result and result.rowcount is not None else 0

            # Log the number of records actually inserted or indicate that no records were inserted
            if inserted_count > 0:
                logging.info(f"{inserted_count} records actually inserted into PostgreSQL.")  # Successful insertion
            else:
                logging.info("No new records were inserted due to duplicates.")  # No insertion due to duplicates

    except Exception as e:
        logging.error(f"PostgreSQL insertion failed: {e}")  # Log any exception that occurs
        raise  # Re-raise the exception to propagate the error



    

def connect_to_postgres(postgres_url: str=MONGO_URL):
    """Connect to PostgreSQL using SQLAlchemy and return the engine."""
    engine = create_engine(postgres_url)
    return engine


def create_table(engine):
    """Create tables based on SQLModel definitions"""
    SQLModel.metadata.create_all(engine)
    print("Table created successfully!")



if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    start_time = time.time()  # Measure total execution time
    
    logging.info("ETL process started.")
    
    try:
        # raise RuntimeError("Simulated general error")
        
        # Extract - Connect to MongoDB and fetch data
        logging.info("Starting data extraction from MongoDB.")
        data = extract_data()
        logging.info(f"Extraction completed. Retrieved {len(data)} documents.")

        # Transform - Process the fetched data into SQL Model objects
        logging.info("Starting data transformation.")
        transformed_data = transform_data(data)
        logging.info(f"Transformation completed. Processed {len(transformed_data)} records.")

        # Load - Connect to PostgreSQL and insert data
        logging.info("Connecting to PostgreSQL.")
        engine = connect_to_postgres()
        
        logging.info("Starting data insertion into PostgreSQL.")
        insert_data(engine, transformed_data)
        logging.info("Data insertion completed successfully.")

    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")

    finally:
        logging.info(f"ETL process finished in {time.time() - start_time:.2f} seconds.")
    

    
    
    
    
    