-- This SQL script creates a new database named tote_OLAP_db and several tables within it.
-- The tables are used to store data for an OLAP system.

DROP DATABASE IF EXISTS tote_OLAP_db;
CREATE DATABASE tote_olap_db;

\c tote_olap_db

CREATE TABLE dim_counterparty (
    counterparty_id INT PRIMARY KEY,
    counterparty_legal_name VARCHAR(240),
    counterparty_legal_address_line_1 VARCHAR(240),
    counterparty_legal_address_line_2 VARCHAR(240),
    counterparty_legal_district VARCHAR(240),
    counterparty_legal_city VARCHAR(240),
    counterparty_legal_postal_code VARCHAR(240),
    counterparty_legal_country VARCHAR(240),
    counterparty_legal_phone_number VARCHAR(240)
);
CREATE TABLE dim_currency (
    currency_id INT PRIMARY KEY,
    currency_code VARCHAR(3),
    currency_name VARCHAR(20)
);
CREATE TABLE dim_design (
    design_id INT PRIMARY KEY,
    design_name VARCHAR(240),
    file_location VARCHAR(240),
    file_name VARCHAR(240)
);
CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    address_line_1 VARCHAR(240),
    address_line_2 VARCHAR(240),
    district VARCHAR(240),
    city VARCHAR(240),
    postal_code VARCHAR(240),
    country VARCHAR(240),
    phone VARCHAR(240)
);
CREATE TABLE dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    payment_type_name VARCHAR(240)
);
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    quarter INT
);
CREATE TABLE dim_staff (
    staff_id INT PRIMARY KEY,
    first_name VARCHAR(240),
    last_name VARCHAR(240),
    department_name VARCHAR(240),
    location VARCHAR(240),
    email_address VARCHAR(240)
);
CREATE TABLE dim_transaction (
    transaction_id INT PRIMARY KEY,
    transaction_type VARCHAR(240),
    sales_order_id INT,
    purchase_order_id INT
);
CREATE TABLE fact_payment (
    payment_record_id INT PRIMARY KEY,
    payment_id INT,
    created_date DATE REFERENCES dim_date(date_id),
    created_time TIME,
    last_updated_date DATE REFERENCES dim_date(date_id),
    last_updated_time TIME,
    transaction_id INT REFERENCES dim_transaction(transaction_id),
    counterparty_id INT REFERENCES dim_counterparty(counterparty_id),
    payment_amount NUMERIC(10,2),
    currency_id INT REFERENCES dim_currency(currency_id),
    payment_type_id INT REFERENCES dim_payment_type(payment_type_id),
    paid BOOLEAN,
    payment_date DATE REFERENCES dim_date(date_id)
);
CREATE TABLE fact_sales_order (
    sales_record_id INT PRIMARY KEY,
    sales_order_id INT,
    created_date DATE REFERENCES dim_date(date_id),
    created_time TIME,
    last_updated_date DATE REFERENCES dim_date(date_id),
    last_updated_time TIME,
    sales_staff_id INT REFERENCES dim_staff(staff_id),
    counterparty_id INT REFERENCES dim_counterparty(counterparty_id),
    units_sold INT,
    unit_price NUMERIC(10,2),
    currency_id INT REFERENCES dim_currency(currency_id),
    design_id INT REFERENCES dim_design(design_id),
    agreed_payment_date DATE REFERENCES dim_date(date_id),
    agreed_delivery_date DATE REFERENCES dim_date(date_id),
    agreed_delivery_location_id INT REFERENCES dim_location(location_id)
);
CREATE TABLE fact_purchase_order (
    purchase_record_id INT PRIMARY KEY,
    purchase_order_id INT,
    created_date DATE REFERENCES dim_date(date_id),
    created_time TIME,
    last_updated_date DATE REFERENCES dim_date(date_id),
    last_updated_time TIME,
    staff_id INT REFERENCES dim_staff(staff_id),
    counterparty_id INT REFERENCES dim_counterparty(counterparty_id),
    item_code VARCHAR(240),
    item_quantity INT,
    item_unit_price NUMERIC(10,2),
    currency_id INT REFERENCES dim_currency(currency_id),
    agreed_delivery_date DATE REFERENCES dim_date(date_id),
    agreed_payment_date DATE REFERENCES dim_date(date_id),
    agreed_delivery_location_id INT REFERENCES dim_location(location_id)
);



