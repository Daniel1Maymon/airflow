from dotenv import load_dotenv
import os, sys
import pandas as pd
from pymongo import MongoClient

# Add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.openai_model import extract_info

# MongoDB connection
client = MongoClient(os.environ.get("MONGO_URL"))
db = client["posts"]
collection =db["collection"]

# mongo = PyMongo()
# result = mongo.db.collection.insert_one(post)




# Query MongoDB for all records
records = collection.find()

# List to store the processed data
processed_data = []


# Process each record
for record in records:
    content = record.get("content", "")
    if content:
        # Send post to GPT:
        structured_data_as_json = extract_info(content)
    
        if "False" not in str(structured_data_as_json) and isinstance(structured_data_as_json, dict) :
            processed_data.append(structured_data_as_json)  # Convert JSON string to Python dictionary


# Save the processed data to CSV
df = pd.DataFrame(processed_data)
df.to_csv("processed_real_estate.csv", index=False, encoding="utf-8")

print("Data has been successfully saved to 'processed_real_estate.csv'")