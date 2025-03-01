#!/bin/bash

# Initialize environment variables
export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=${AIRFLOW_HOME}/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db init

# Create admin user
echo "Creating admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Air \
    --lastname Flow \
    --role Admin \
    --email admin@example.com

# Start Airflow services
echo "Starting Airflow webserver and scheduler..."
airflow webserver --port 8080 &  # Run webserver in the background
airflow scheduler &              # Run scheduler in the background

# List available DAGs
echo "Listing all DAGs..."
airflow dags list

# End of script
echo "Airflow setup completed successfully!"
