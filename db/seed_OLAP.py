"""This script loads data from JSON files and inserts it into an OLAP database.

This script connects to an OLAP database using the pg8000 library and loads data from several JSON files. The data is then inserted into the corresponding tables in the OLAP database. The script includes error handling for both the connection to the database and for inserting data into the tables. If an error occurs while connecting to the database, the error message will be printed. If an error occurs while inserting data into a table, the error message and the row that caused the error will be printed.

Environment variables:
    LOCAL_HOST (str): The hostname of the OLAP database.
    LOCAL_PORT (int): The port number of the OLAP database.
    LOCAL_USER (str): The username for connecting to the OLAP database.
    LOCAL_DB_OLAP (str): The name of the OLAP database.

Raises:
    Exception: If an error occurs while connecting to the database or inserting data into a table.
"""


import pg8000.native
from dotenv import load_dotenv
import json
import os

load_dotenv()

host = os.environ['LOCAL_HOST']
port = os.environ['LOCAL_PORT']
user = os.environ['LOCAL_USER']
db = os.environ['LOCAL_DB_OLAP']

try:
    conn = pg8000.Connection(
        user=user, host=host, database=db, port=port)
    cursor = conn.cursor()

    # Load data from JSON file
    print('Seeding dim_counterparty table.....')
    with open('db/OLAP_Data/dim_counterparty.json', 'r') as f:
        data = json.load(f)

    # Insert data into dim_counterparty table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO dim_counterparty (counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, counterparty_legal_address_line_2, counterparty_legal_district, 
                counterparty_legal_city, 
                counterparty_legal_postal_code, counterparty_legal_country, counterparty_legal_phone_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['counterparty_id'],
                 row['counterparty_legal_name'],
                 row['counterparty_legal_address_line_1'],
                 row['counterparty_legal_address_line_2'],
                 row['counterparty_legal_district'],
                 row['counterparty_legal_city'],
                 row['counterparty_legal_postal_code'],
                 row['counterparty_legal_country'],
                 row['counterparty_legal_phone_number'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding dim_currency table.....')
    with open('db/OLAP_Data/dim_currency.json', 'r') as f:
        data = json.load(f)

    # Insert data into dim_currency table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO dim_currency (currency_id, currency_code, currency_name)
                VALUES (%s, %s, %s)
                """,
                (row['currency_id'], row['currency_code'], row['currency_name'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding dim_design table.....')
    with open('db/OLAP_Data/dim_design.json', 'r') as f:
        data = json.load(f)

    # Insert data into dim_design table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO dim_design (design_id, design_name, file_location, file_name)
                VALUES (%s, %s, %s, %s)
                """,
                (row['design_id'], row['design_name'],
                 row['file_location'], row['file_name'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding dim_location table.....')
    with open('db/OLAP_Data/dim_location.json', 'r') as f:
        data = json.load(f)

    # Insert data into dim_location table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO dim_location (location_id, address_line_1, address_line_2, district, city, postal_code, country, phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['location_id'], row['address_line_1'], row['address_line_2'],
                 row['district'], row['city'], row['postal_code'], row['country'], row['phone'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding dim_payment_type table.....')
    with open('db/OLAP_Data/dim_payment_type.json', 'r') as f:
        data = json.load(f)

    # Insert data into dim_payment_type table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO dim_payment_type (payment_type_id, payment_type_name)
                VALUES (%s, %s)
                """,
                (row['payment_type_id'], row['payment_type_name'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding dim_date table.....')
    with open('db/OLAP_Data/dim_date.json', 'r') as f:
        data = json.load(f)

    # Insert data into dim_date table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['date_id'], row['year'], row['month'], row['day'],
                 row['day_of_week'], row['day_name'], row['month_name'], row['quarter'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding dim_staff table.....')
    with open('db/OLAP_Data/dim_staff.json', 'r') as f:
        data = json.load(f)

    # Insert data into dim_staff table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO dim_staff (staff_id, first_name, last_name, department_name, location, email_address)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (row['staff_id'], row['first_name'], row['last_name'],
                 row['department_name'], row['location'], row['email_address'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding dim_transaction table.....')
    with open('db/OLAP_Data/dim_transaction.json', 'r') as f:
        data = json.load(f)

    # Insert data into dim_transaction table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO dim_transaction (transaction_id, transaction_type, sales_order_id, purchase_order_id)
                VALUES (%s, %s, %s, %s)
                """,
                (row['transaction_id'], row['transaction_type'],
                 row['sales_order_id'], row['purchase_order_id'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding fact_payment table.....')
    with open('db/OLAP_Data/fact_payment.json', 'r') as f:
        data = json.load(f)

    # Insert data into fact_payment table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO fact_payment (payment_record_id, payment_id, created_date, created_time, last_updated_date, last_updated_time, transaction_id, counterparty_id, payment_amount, currency_id, payment_type_id, paid, payment_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['payment_record_id'], row['payment_id'], row['created_date'], row['created_time'], row['last_updated_date'], row['last_updated_time'],
                 row['transaction_id'], row['counterparty_id'], row['payment_amount'], row['currency_id'], row['payment_type_id'], row['paid'], row['payment_date'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding fact_sales_order table.....')
    with open('db/OLAP_Data/fact_sales_order.json', 'r') as f:
        data = json.load(f)

    # Insert data into fact_sales_order table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO fact_sales_order (sales_record_id, sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['sales_record_id'], row['sales_order_id'], row['created_date'], row['created_time'], row['last_updated_date'], row['last_updated_time'], row['sales_staff_id'], row['counterparty_id'],
                 row['units_sold'], row['unit_price'], row['currency_id'], row['design_id'], row['agreed_payment_date'], row['agreed_delivery_date'], row['agreed_delivery_location_id'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Load data from JSON file
    print('Seeding fact_purchase_order table.....')
    with open('db/OLAP_Data/fact_purchase_order.json', 'r') as f:
        data = json.load(f)

    # Insert data into fact_purchase_order table
    for row in data:
        try:
            cursor.execute(
                """
                INSERT INTO fact_purchase_order (purchase_record_id, purchase_order_id, created_date, created_time, last_updated_date, last_updated_time, staff_id, counterparty_id, item_code, item_quantity, item_unit_price, currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['purchase_record_id'], row['purchase_order_id'], row['created_date'], row['created_time'], row['last_updated_date'], row['last_updated_time'], row['staff_id'], row['counterparty_id'],
                 row['item_code'], row['item_quantity'], row['item_unit_price'], row['currency_id'], row['agreed_delivery_date'], row['agreed_payment_date'], row['agreed_delivery_location_id'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    print('\nAll data inserted into the database\n')
    conn.commit()
    cursor.close()
    conn.close()

except Exception as e:
    print(f'Error: {e}')
