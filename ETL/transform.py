import logging
import pandas as pd
from ETL.models.Post import Post
from utils.regex_extractor import extract_rental_info

def transform_data(data: list) -> list:
    """Transform extracted data into SQL Model objects."""
    logging.info("Starting data transformation...")

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

    logging.info(f"Transformation completed. Processed {len(processed_data)} records.")
    return processed_data
