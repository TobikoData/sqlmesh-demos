from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import Row
from faker import Faker
import uuid
from datetime import datetime
import databricks.connect

# Configure Databricks Connect
import os
from databricks.connect import DatabricksSession


server_hostname = "dbc-b9f590c4-0a08.cloud.databricks.com"
cluster_id = "0603-211256-ns7ii2e2"

dbc = DatabricksSession.builder.remote(
  host       = f"https://{server_hostname}",
  token      = os.getenv('DATABRICKS_ACCESS_TOKEN'),
  cluster_id = cluster_id
).getOrCreate()


fake = Faker()

# Define the schema for the DataFrame
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_name", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True)
])

def generate_fake_data(num_rows: int):
    data = []
    for _ in range(num_rows):
        event_id = str(uuid.uuid4())
        event_name = fake.word()
        event_timestamp = datetime.now()
        user_id = str(uuid.uuid4())
        row = Row(event_id=event_id, event_name=event_name, event_timestamp=event_timestamp, user_id=user_id)
        data.append(row)
    return data


def create_schema_if_not_exists(schema_name: str):
    dbc.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def append_to_databricks_table(table_name: str, num_rows: int):
    # Generate 5 rows of fake data
    fake_data = generate_fake_data(num_rows)
    
    # Create a Spark DataFrame
    df = dbc.createDataFrame(fake_data, schema)

    # Create the schema if it doesn't exist
    schema_name = table_name.split('.')[0] + '.' + table_name.split('.')[1]
    create_schema_if_not_exists(schema_name)

    # Append the data to the Databricks catalog
    df.write.mode("append").saveAsTable(table_name)

    print(f"5 rows of fake data appended to {table_name}")

# Call the function to append data
append_to_databricks_table(table_name="public_demo.raw_data.demo_events", num_rows=3)
