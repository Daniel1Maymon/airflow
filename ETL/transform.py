import logging
import os, sys
from typing import List, Dict
from pymongo import MongoClient
from bson import ObjectId
from airflow.models import Variable
import pandas as pd

# Define base directory (one level up from current file)
current_file_dir = os.path.dirname(__file__)
base_dir = os.path.abspath(os.path.join(current_file_dir, '..'))
print(f"current_file_dir={current_file_dir}")
print(f"base_dir={base_dir}")

# Add base directory to system path
sys.path.append(base_dir)

from ETL.models.Post import Post
from utils.regex_extractor import extract_rental_info

MONGO_DB = "posts"
RAW_COLLECTION = "raw_posts"
IDS_COLLECTION = "posts_to_process_ids"
TRANSFORMED_COLLECTION = "transformed_posts"


def transform_data(data: List[Dict]) -> List[Post]:
    if not data:
        logging.warning("No data received for transformation.")
        return []

    df = pd.DataFrame(data)

    if 'content' not in df.columns:
        logging.error("'content' field missing from input data.")
        return []

    df[['price', 'rooms', 'size']] = df['content'].apply(
        lambda x: pd.Series(extract_rental_info(x))
    )

    df = df[['content', 'link', 'rooms', 'size', 'price']]

    processed_data = []

    for i, row in df.iterrows():
        row_data = row.to_dict()
        row_data["mongo_id"] = str(data[i]["_id"])

        for key in ['rooms', 'size', 'price']:
            row_data[key] = None if pd.isna(row_data.get(key)) else row_data.get(key)

        try:
            post = Post(**row_data)
            processed_data.append(post)
        except Exception as e:
            logging.warning(f"Failed to create Post object for row {i}: {e}")

    logging.info(f"Transformation completed. Processed {len(processed_data)} records.")
    return processed_data


def get_mongo_url() -> str:
    return Variable.get("MONGO_URL", default_var=os.getenv("MONGO_URL", "mongodb://localhost:27017/"))


def run_transform() -> None:
    client = MongoClient(get_mongo_url())
    db = client[MONGO_DB]

    try:
        batch_doc = db[IDS_COLLECTION].find_one()
        if not batch_doc or "post_ids" not in batch_doc:
            logging.info("No post IDs found for transformation.")
            return

        post_ids = batch_doc["post_ids"]
        if not isinstance(post_ids, list) or not all(post_ids):
            logging.warning("Invalid or empty post_ids in batch.")
            return

        try:
            object_ids = [ObjectId(_id) for _id in post_ids]
        except Exception as e:
            logging.error(f"Failed to convert post_ids to ObjectId: {e}")
            return

        raw_posts = list(db[RAW_COLLECTION].find({"_id": {"$in": object_ids}}))
        if not raw_posts:
            logging.info("No matching raw posts found in raw_posts.")
            return

        transformed = transform_data(raw_posts)
        if not transformed:
            logging.info("No posts were transformed.")
            return

        documents = [obj.dict() for obj in transformed]
        db[TRANSFORMED_COLLECTION].insert_many(documents, ordered=False)
        logging.info(f"Inserted {len(documents)} transformed posts into '{TRANSFORMED_COLLECTION}'.")

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise
    finally:
        client.close()
        logging.info("MongoDB connection closed after transformation.")


if __name__ == '__main__':
    run_transform()