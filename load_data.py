import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import pymongo
import json
import os
from aws_util import save_log_to_s3

# log_to_cloudwatch("Starting data loading to MongoDB")
# initialize_cloudwatch_log_group_and_stream()



# Function to get MongoDB credentials from AWS Secrets Manager
def get_mongo_credentials():
    secret_name = "zohocrmmig"
    region_name = "ap-southeast-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Parse the secret string
    secret = json.loads(get_secret_value_response['SecretString'])
    print("Retrieved secret string:", secret)  # Print the raw secret string

    # Check if all required keys are present
    required_keys = ['username', 'password', 'host', 'port']
    for key in required_keys:
        if key not in secret:
            raise KeyError(f"Missing key in secret: {key}")

    return secret['username'], secret['password'], secret['host'], secret['port']

# Function to connect to MongoDB and load data into 'leads' collection
def load_data_to_mongodb():
    transformed_data = "transformed_leads"
    leads_data = "leads_data"
    
    # Fetch MongoDB credentials
    username, password, host, port = get_mongo_credentials()
    
    # Specify the database name
    database = "zoho_crm"  # Replace with your desired database name

    # MongoDB connection URI
    mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?tls=true&retryWrites=false&tlsCAFile=/home/ubuntu/etl/zoho-etl-script/etl/global-bundle.pem"
    
    # Connect to MongoDB
    client = pymongo.MongoClient(mongo_uri)
    db = client[database]

    # Check if the 'leads' collection exists, if not, create it
    collection_name = "leads"
    if collection_name not in db.list_collection_names():
        print(f"Creating collection '{collection_name}' in MongoDB.")
        db.create_collection(collection_name)
    else:
        print(f"Collection '{collection_name}' already exists in MongoDB.")

    # Load transformed data from JSON file
    json_file_path = f'/home/ubuntu/etl/zoho-etl-script/etl/{leads_data}.json'
    if not os.path.exists(json_file_path):
        print(f"File '{json_file_path}' not found. Please ensure data is fetched from Zoho CRM.")
        return
    
    with open(json_file_path, 'r') as file:
        leads_data = json.load(file)
    
    # Insert data into the 'leads' collection
    db[collection_name].insert_many(leads_data)
    print(f"Inserted {len(leads_data)} documents into the '{collection_name}' collection.")

    inserted_count = len(leads_data)
    log_entry = {
        "stage": "Loading",
        "timestamp": str(datetime.now()),
        "record_count": inserted_count,
        "status": "Data loaded into MongoDB"
    }
    save_log_to_s3(log_entry)

# Run the function
if __name__ == "__main__":
    load_data_to_mongodb()
