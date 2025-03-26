import logging, os
from pymongo import MongoClient, errors
from airflow.models import Variable

MONGO_DB = "posts"
SOURCE_COLLECTION = "collection"
TARGET_COLLECTION = "etl_extracted"

def get_mongo_url():
    """Get mongo url first from Airflow Variable, else from environment."""
    mongo_url = Variable.get("MONGO_URL", default_var=None)
    if mongo_url:
        return mongo_url

    mongo_url = os.getenv("MONGO_URL")
    if mongo_url:
        return mongo_url

    raise ValueError("MONGO_URL must be set in Airflow Variables or environment.")

def run_extract(mongo_url: str = None, limit: int = 20) -> None:
    """Extract the latest N records from source_collection into etl_extracted."""
    mongo_url = get_mongo_url()
    client = MongoClient(mongo_url)
    if not mongo_url:
        raise ValueError("MONGO_URL must be set either in Airflow Variables or environment variables.")

    client = MongoClient(mongo_url)

    try:
        logging.info("Connecting to MongoDB for extraction.")
        db = client[MONGO_DB]
        source_coll = db[SOURCE_COLLECTION]
        target_coll = db[TARGET_COLLECTION]

        # Sort by '_id' descending to get latest documents
        cursor = source_coll.find().sort("_id", -1).limit(limit)

        records = list(cursor)
        if not records:
            logging.info("No records found for extraction.")
            return

        # Step 1: Get all _id's from extracted records
        record_ids = [record["_id"] for record in records]

        # Step 2: Find existing records in MongoDB
        existing_docs_cursor = target_coll.find({"_id": {"$in": record_ids}}, {"_id": 1})

        # Step 3: Create a set of existing _id's
        existing_ids = set()
        for doc in existing_docs_cursor:
            existing_ids.add(doc["_id"])


        new_records = [record for record in records if record['_id'] not in existing_ids]

        if new_records:
            target_coll.insert_many(new_records, ordered=False)
            logging.info(f"Inserted {len(new_records)} new documents into '{TARGET_COLLECTION}'.")
        else:
            logging.info("No new records to insert.")

    except errors.BulkWriteError as bwe:
        logging.error(f"Bulk write error during insertion: {bwe.details}")
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise
    finally:
        client.close()
        logging.info("MongoDB connection closed after extraction.")
