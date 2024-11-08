import json
import boto3
from botocore.exceptions import ClientError
from pymongo import MongoClient
from datetime import datetime
from extract_data import fetch_leads
from aws_util import save_log_to_s3

# log_to_cloudwatch("Starting data validation from Zoho CRM")

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

# Function to get the leads collection from MongoDB
def get_leads_collection():
    database = "zoho_crm"
    username, password, host, port = get_mongo_credentials()
    mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?tls=true&retryWrites=false&tlsCAFile=/home/ubuntu/etl/zoho-etl-script/etl/global-bundle.pem"
    
    client = MongoClient(mongo_uri)
    db = client[database]
    collection_name = "leads"
    
    # Ensure the leads collection exists
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
        
    return db[collection_name]

# Fetch lead data from MongoDB
def get_mongo_leads():
    leads_collection = get_leads_collection()
    leads = list(leads_collection.find({}, {"_id": 0}))
    return {lead["Email"]: lead for lead in leads}

# Compare Zoho data with MongoDB
def validate_data():
    mongo_leads = get_mongo_leads()
    zoho_leads = fetch_leads(max_records=200)  # Re-fetch from Zoho to ensure freshness

    discrepancies = []
    required_fields = ["Email", "Lead_Status", "Phone"]  # Required fields for validation

    for zoho_lead in zoho_leads:
        email = zoho_lead.get("Email")
        mongo_lead = mongo_leads.get(email)
        
        if not mongo_lead:
            discrepancies.append({"Email": email, "error": "Missing in MongoDB"})
            continue

        for field in required_fields:
            if mongo_lead.get(field) != zoho_lead.get(field):
                discrepancies.append({
                    "Email": email,
                    "field": field,
                    "zoho_value": zoho_lead.get(field),
                    "mongo_value": mongo_lead.get(field)
                })

    # Log discrepancies
    if discrepancies:
        with open("/home/ubuntu/etl/zoho-etl-script/etl/discrepancies.json", "w") as file:
            json.dump(discrepancies, file)
        print("Discrepancies found. Details saved to discrepancies.json.")

        log_entry = {
            "stage": "Validation",
            "timestamp": str(datetime.now()),
            "status": "Discrepancies found in validation",
            "discrepancies": discrepancies
        }
        save_log_to_s3(log_entry)

    else:
        print("Data validation successful. No discrepancies found.")

        log_entry = {
            "stage": "Validation",
            "timestamp": str(datetime.now()),
            "status": "Validation successful, no discrepancies found"
        }
        save_log_to_s3(log_entry)
        

if __name__ == "__main__":
    validate_data()
