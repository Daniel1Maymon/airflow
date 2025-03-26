import os
import sys
# Adjusting the system path for ETL imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ETL.extract import run_extract


# Set environment variables for local debugging
# os.environ["MONGO_URL"] = "mongodb://localhost:27017"  # Adjust if needed

if __name__ == "__main__":
    print("==== DEBUGGING EXTRACT TASK ====")
    # extract_task()
    run_extract()

    print("\n==== DEBUGGING TRANSFORM TASK ====")
    # transform_task()
