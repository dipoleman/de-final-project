"""This script extracts data from a PostgreSQL database and saves it to JSON files.

This script uses the `pg8000` library to connect to a PostgreSQL database and extract data from multiple tables. The data is then saved to JSON files, one for each table. The script also includes a custom JSON encoder function to handle `datetime` and `Decimal` objects.

This script requires the following environment variables to be set:
- `LOCAL_HOST`: The hostname of the local PostgreSQL server
- `LOCAL_PORT`: The port number of the local PostgreSQL server
- `LOCAL_USER`: The username for the local PostgreSQL server
- `LOCAL_DB`: The name of the local PostgreSQL database

This script also requires the following libraries to be installed:
- `pg8000`
- `boto3`
- `dotenv`
"""

from src.lambda_functions.warehouse_lambda import get_secret
import pg8000.native
import boto3
import json
import os
from dotenv import load_dotenv
import datetime
from decimal import Decimal

load_dotenv()


def datetime_handler(obj):
    """Custom JSON encoder function for datetime and Decimal objects.

    Args:
        obj: The object to be encoded.

    Returns:
        str: A string representation of the object.

    Raises:
        TypeError: If the object is not of type datetime.datetime, datetime.date, datetime.time, or Decimal.
    """
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, datetime.time):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        raise TypeError(
            f'Object of type {obj.__class__.__name__} is not JSON serializable')


secret_string = get_secret()
warehouse = json.loads(secret_string)
user = warehouse["User"]
host = warehouse["Host"]
db = warehouse["Schema"]
port = warehouse["Port"]
password = warehouse["Password"]

s3 = boto3.client("s3")

local_host = os.environ['LOCAL_HOST']
local_port = os.environ['LOCAL_PORT']
local_user = os.environ['LOCAL_USER']
local_db = os.environ['LOCAL_DB']

tables = ['dim_counterparty', 'dim_currency', 'dim_design', 'dim_location', 'dim_staff',
          'dim_payment_type', 'dim_date', 'dim_transaction', 'fact_sales_order', 'fact_purchase_order', 'fact_payment']
conn = pg8000.Connection(
    user, host=host, database=db, port=port, password=password)

for table in tables:
    cursor = conn.cursor()
    data_query = f'''SELECT * FROM {table};'''
    cursor.execute(data_query)
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    data = [dict(zip(column_names, row)) for row in rows]
    with open(f'db/OLAP_data/{table}.json', 'w') as f:
        json.dump(data, f, default=datetime_handler)

conn.close()
