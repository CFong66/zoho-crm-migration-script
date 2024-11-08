#!/bin/bash

# Navigate to the ETL directory
cd /home/ubuntu/etl/zoho-etl-script

# Set up virtual environment (optional)
python3 -m venv etl_env
source etl_env/bin/activate

# Install dependencies if needed (assuming requirements.txt exists)
# pip install -r requirements.txt

# Run the ETL scripts in the required order
echo "Starting data extraction..."
if python3 /home/ubuntu/etl/zoho-etl-script/extract_data.py; then
    echo "Extraction completed."
    sleep 30
else
    echo "Extraction failed. Exiting."
    exit 1

echo "Validating data..."
if python3 /home/ubuntu/etl/zoho-etl-script/validate_data.py; then
    echo "Validation completed."
    sleep 30
else
    echo "Validation failed. Exiting."
    exit 1

echo "Transforming data..."
if python3 /home/ubuntu/etl/zoho-etl-script/transform_data.py; then
    echo "Transformation completed."
    sleep 30
else
    echo "Transformation failed. Exiting."
    exit 1

echo "Loading data..."
if python3 /home/ubuntu/etl/zoho-etl-script/load_data.py; then
    echo "Loading completed."
    sleep 30
else
    echo "Loading failed. Exiting."
    exit 1

echo "ETL process completed."

# Deactivate the virtual environment
deactivate