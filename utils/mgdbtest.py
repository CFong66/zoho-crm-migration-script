import pymongo
import boto3
from botocore.exceptions import ClientError
import json

# Specify the database name
db_name = "zoho_crm"
# Specify the collection name
col_name = "leads"

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

def connect_to_docdb(client):
    # Fetch MongoDB credentials
    username, password, host, port = get_mongo_credentials()

    # MongoDB connection URI
    uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}?retryWrites=false&tlsCAFile=/home/ubuntu/etl/zoho-etl-script/etl/global-bundle.pem"

    try:
        client = pymongo.MongoClient(uri = uri)
        db =client[db_name]
        print("Connected to DocumentDB")
        return db
    
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print("Failed to connect to DocumentDB:", err)
        return None

# Fetch all records from the leads collection
def fetch_all_leads(db):
    try:
        leads_collection = db[col_name]
        leads = leads_collection.find({})
        for lead in leads:
            print(lead)
    except Exception as e:
        print("Error fetching leads:", e)

# Update records in the leads collection
def update_lead(db, lead_id, update_fields):
    try:
        leads_collection = db[col_name]
        result = leads_collection.update_one(
            {"_id": lead_id},  # Condition to match specific lead
            {"$set": update_fields}  # Fields to update
        )
        if result.modified_count > 0:
            print(f"Lead with _id {lead_id} updated successfully.")
        else:
            print(f"No lead found with _id {lead_id}.")
    except Exception as e:
        print("Error updating lead:", e)

# Delete records from the leads collection
def delete_lead(db, lead_id):
    try:
        leads_collection = db[col_name]
        result = leads_collection.delete_one({"_id": lead_id})
        if result.deleted_count > 0:
            print(f"Lead with _id {lead_id} deleted successfully.")
        else:
            print(f"No lead found with _id {lead_id}.")
    except Exception as e:
        print("Error deleting lead:", e)

# Main function
if __name__ == "__main__":
    db = connect_to_docdb(client)
    if db:
        # Fetch all leads
        print("Fetching all leads:")
        fetch_all_leads(db)

        # # Update a lead's information (example: changing lead status)
        # lead_id = "specific-lead-id"  # Replace with the actual lead ID
        # update_fields = {"Lead_Status": "Updated Status"}
        # update_lead(db, lead_id, update_fields)

        # # Delete a lead record
        # delete_lead(db, lead_id)