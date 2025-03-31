# init_db.py

import os
import sys

# Step 1: Set up path to project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(PROJECT_ROOT)

# Step 2: Import models (required for metadata discovery)
from sqlmodel import SQLModel, create_engine
from ETL.models.Post import Post  # Needed for SQLModel to detect the table

# Step 3: Define DB connection
DATABASE_URL = "postgresql+psycopg2://daniel:daniel@localhost:5431/rentals_db"
engine = create_engine(DATABASE_URL)

# Step 4: Create all tables
def init_db():
    SQLModel.metadata.create_all(engine)
    print("Database initialized successfully.")

if __name__ == "__main__":
    init_db()
