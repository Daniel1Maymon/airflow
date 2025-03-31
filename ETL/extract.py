from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
DB_NAME = "posts"
RAW_COLLECTION = "raw_posts"
IDS_COLLECTION = "posts_to_process_ids"
BATCH_SIZE = 3

def run_extract(**kwargs):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    if db[IDS_COLLECTION].count_documents({}) > 0:
        print("Batch already in progress. Skipping extract.")
        return

    posts_cursor = db[RAW_COLLECTION].find(
        {"processed": False},
        {"_id": 1}
    ).limit(BATCH_SIZE)

    post_ids = [doc["_id"] for doc in posts_cursor]

    if not post_ids:
        print("No unprocessed posts found.")
        return

    db[IDS_COLLECTION].insert_one({"post_ids": post_ids})
    print(f"Extracted {len(post_ids)} post IDs.")

if __name__ == "__main__":
    run_extract()