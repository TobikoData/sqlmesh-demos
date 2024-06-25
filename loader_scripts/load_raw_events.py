"""This is a public demo script to generate demo data """
import pandas as pd
import uuid
from datetime import datetime, timedelta
import random
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json

# set credentials
SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SQLMESH_CREDENTIALS"])

# Define the list of possible event names
event_names = ["page_view", "product_view", "ad_view", "video_view", "blog_view"]

def generate_fake_data(num_rows: int, end_date: str):
    end_date_parsed = datetime.strptime(end_date, '%Y-%m-%d')
    data = []
    for i in range(num_rows):
        event_id = str(uuid.uuid4())
        event_name = random.choice(event_names)
        event_timestamp = end_date_parsed
        user_id = str(uuid.uuid4())
        row = {
            "event_id": event_id,
            "event_name": event_name,
            "event_timestamp": event_timestamp,
            "user_id": user_id
        }
        data.append(row)
    return data

def create_table_if_not_exists(client, dataset_name: str, table_name: str):
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    # Check if the table exists
    try:
        client.get_table(table_ref)
    except:
        schema = [
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table.table_id}")

def append_to_bigquery_table(table_name: str, num_rows: int, end_date: str, project_id: str):
    # Authenticate with BigQuery using environment variable
    service_account_info = SERVICE_ACCOUNT_INFO
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Parse table name
    dataset_name, table_name = table_name.split('.')

    # Generate fake data
    fake_data = generate_fake_data(num_rows, end_date)

    # Convert the data into a DataFrame
    df = pd.DataFrame(fake_data)

    # Create the table if it doesn't exist
    create_table_if_not_exists(client, dataset_name, table_name)

    # Append the data to the BigQuery table
    job = client.load_table_from_dataframe(df, f"{dataset_name}.{table_name}")
    job.result()  # Wait for the job to complete

    print(f"{num_rows} rows of raw events demo data with date [{end_date}] appended to {dataset_name}.{table_name}")

# Example usage
append_to_bigquery_table(
    table_name="tcloud_raw_data.raw_events",
    num_rows=5,
    end_date="2024-06-24",
    project_id="sqlmesh-public-demo"
)
