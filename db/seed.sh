#!/bin/bash

# This script sets up the environment and runs the SQL and Python scripts to create and seed the OLTP and OLAP databases.

# Set the PYTHONPATH environment variable to the current working directory
export PYTHONPATH="$PWD"

# Load the values from the .env file into environment variables
export $(grep -v '^#' .env | xargs)

# Pass the values of the environment variables to the .sql file using the -v option
psql -v db_user=$DB_USER -v db_password=$DB_PASSWORD -f db/create_OLTP.sql

# Run the Python script to seed the OLTP database
python db/seed_OLTP.py

# Pass the values of the environment variables to the .sql file using the -v option
psql -v db_user=$DB_USER -v db_password=$DB_PASSWORD -f db/create_OLAP.sql

# Run the Python script to seed the OLAP database
python db/seed_OLAP.py
