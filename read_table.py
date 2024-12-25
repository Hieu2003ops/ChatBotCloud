from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
import os
from utils.logger import AppLog

# Load environment variables from .env file
load_dotenv()

def create_keyfile_dict():
    variables_keys = {
        "type": os.getenv("TYPE"),
        "project_id": os.getenv("PROJECT_ID"),
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": os.getenv("PRIVATE_KEY"),
        "client_email": os.getenv("CLIENT_EMAIL"),
        "client_id": os.getenv("CLIENT_ID"),
        "auth_uri": os.getenv("AUTH_URI"),
        "token_uri": os.getenv("TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL")
    }
    return variables_keys

async def read_table():
    # Get environment variables
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    table_id = os.getenv("TABLE_ID")

    # Log the details for debugging
    AppLog.info(f"Project ID: {project_id}")
    AppLog.info(f"Dataset ID: {dataset_id}")
    AppLog.info(f"Table ID: {table_id}")

    # Check for missing environment variables
    if not project_id or not dataset_id or not table_id:
        raise ValueError("Ensure PROJECT_ID, DATASET_ID, and TABLE_ID are set in environment variables.")

    # Combine to form the full table reference
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    AppLog.info(f"Full Table ID: {full_table_id}")

    # Load JSON content of the service account key file from environment variable
    service_account_json = create_keyfile_dict()

    # Create BigQuery client from JSON
    try:
        client = bigquery.Client.from_service_account_info(service_account_json)
        AppLog.info("BigQuery client created successfully.")
    except Exception as e:
        AppLog.info(f"Error creating BigQuery client: {e}")
        return

    # Query to select the first 10 rows
    query = f"SELECT * FROM `{full_table_id}` LIMIT 10"

    try:
        query_job = client.query(query)
        results = query_job.result()  # Wait for the job to complete

        # Print the rows
        for row in results:
            print(dict(row))  # Convert Row object to dictionary for better readability
    except Exception as e:
        AppLog.info(f"An error occurred while querying the table: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(read_table())
