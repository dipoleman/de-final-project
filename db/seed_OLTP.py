"""This script loads data from JSON files and inserts it into an OLTP database.

This script connects to an OLTP database using the pg8000 library and loads data from several JSON files. The data is then inserted into the corresponding tables in the OLTP database. The script includes error handling for both the connection to the database and for inserting data into the tables. If an error occurs while connecting to the database, the error message will be printed. If an error occurs while inserting data into a table, the error message and the row that caused the error will be printed.

Environment variables:
    LOCAL_HOST (str): The hostname of the OLTP database.
    LOCAL_PORT (int): The port number of the OLTP database.
    LOCAL_USER (str): The username for connecting to the OLTP database.
    LOCAL_DB_OLAP (str): The name of the OLTP database.

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
db = os.environ['LOCAL_DB_OLTP']

try:
    conn = pg8000.Connection(
        user=user, host=host, database=db, port=port)
    cursor = conn.cursor()

    # Seed address table
    print('Seeding address table.....')
    with open('db/OLTP_Data/address.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO address (address_id, address_line_1, address_line_2, district, city, postal_code, country, phone, created_at, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (row['address_id'], row['address_line_1'], row['address_line_2'], row['district'], row['city'],
                 row['postal_code'], row['country'], row['phone'], row['created_at'], row['last_updated'])
            )

        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Seed counterparty table
    print('Seeding counterparty table.....')
    with open('db/OLTP_Data/counterparty.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO counterparty (counterparty_id, counterparty_legal_name, legal_address_id, commercial_contact, delivery_contact, created_at,last_updated) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (row['counterparty_id'], row['counterparty_legal_name'], row['legal_address_id'],
                 row['commercial_contact'], row['delivery_contact'], row['created_at'], row['last_updated'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Seed currency table
    print('Seeding currency table.....')
    with open('db/OLTP_Data/currency.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO currency (currency_id,currency_code ,created_at,last_updated) VALUES (%s,%s,%s,%s)",
                (row['currency_id'], row['currency_code'],
                 row['created_at'], row['last_updated'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Seed department table
    print('Seeding department table.....')
    with open('db/OLTP_Data/department.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO department (department_id, department_name, location, manager, created_at,last_updated) VALUES (%s,%s,%s,%s,%s,%s)",
                (row['department_id'], row['department_name'], row['location'],
                 row['manager'], row['created_at'], row['last_updated'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Seed staff table
    print('Seeding staff table.....')
    with open('db/OLTP_Data/staff.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO staff (staff_id, first_name, last_name, department_id, email_address, created_at, last_updated) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (row['staff_id'], row['first_name'], row['last_name'], row['department_id'],
                 row['email_address'], row['created_at'], row['last_updated'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Seed design table
    print('Seeding design table.....')
    with open('db/OLTP_Data/design.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO design (design_id, created_at, design_name,file_location,file_name,last_updated) VALUES (%s,%s,%s,%s,%s,%s)",
                (row['design_id'], row['created_at'], row['design_name'],
                 row['file_location'], row['file_name'], row['last_updated'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Seed payment_type table
    print('Seeding payment_type table.....')
    with open('db/OLTP_Data/payment_type.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO payment_type (payment_type_id, payment_type_name, created_at, last_updated) VALUES (%s, %s, %s, %s)",
                (row['payment_type_id'], row['payment_type_name'],
                 row['created_at'], row['last_updated'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

   # Seed purchase_order table
    print('Seeding purchase_order table.....')
    with open('db/OLTP_Data/purchase_order.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO purchase_order (purchase_order_id, created_at, last_updated, staff_id, counterparty_id, item_code, item_quantity, item_unit_price, currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (row['purchase_order_id'], row['created_at'], row['last_updated'], row['staff_id'], row['counterparty_id'], row['item_code'], row['item_quantity'],
                 row['item_unit_price'], row['currency_id'], row['agreed_delivery_date'], row['agreed_payment_date'], row['agreed_delivery_location_id'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")
        conn.commit()

    # Seed sales_order table
    print('Seeding sales_order table.....')
    with open('db/OLTP_Data/sales_order.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO sales_order (sales_order_id, created_at, last_updated, design_id, staff_id, counterparty_id, units_sold, unit_price, currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (row['sales_order_id'], row['created_at'], row['last_updated'], row['design_id'], row['staff_id'], row['counterparty_id'], row['units_sold'],
                 row['unit_price'], row['currency_id'], row['agreed_delivery_date'], row['agreed_payment_date'], row['agreed_delivery_location_id'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Seed transaction table
    print('Seeding transaction table.....')
    with open('db/OLTP_Data/transaction.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO transaction (transaction_id, transaction_type, sales_order_id, purchase_order_id, created_at, last_updated) VALUES (%s,%s,%s,%s,%s,%s)",
                (row['transaction_id'], row['transaction_type'], row['sales_order_id'],
                 row['purchase_order_id'], row['created_at'], row['last_updated'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    # Seed payment table
    print('Seeding payment table.....')
    with open('db/OLTP_Data/payment.json', 'r') as f:
        data = json.load(f)

    for row in data:
        try:
            cursor.execute(
                "INSERT INTO payment (payment_id, created_at, last_updated, transaction_id, counterparty_id, payment_amount, currency_id, payment_type_id, paid, payment_date, company_ac_number, counterparty_ac_number) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (row['payment_id'], row['created_at'], row['last_updated'], row['transaction_id'], row['counterparty_id'], row['payment_amount'],
                 row['currency_id'], row['payment_type_id'], row['paid'], row['payment_date'], row['company_ac_number'], row['counterparty_ac_number'])
            )
        except Exception as e:
            print(f"Error inserting row {row}: {e}")

    print('\nAll data inserted into the database\n')
    conn.commit()
    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error connecting to database: {e}")
