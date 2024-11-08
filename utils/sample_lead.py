import requests
import json
from faker import Faker
import boto3
from TF.intership.zoho.GPT.script.etl.aws_util import log_error, send_metrics_to_cloudwatch

# Set up Faker instance
fake = Faker()


# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/

from botocore.exceptions import ClientError


def get_secret():

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
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']

    # Your code goes here.



# Initialize AWS Secrets Manager client
secrets_client = boto3.client('secretsmanager', region_name='ap-southeast-2')

# Retrieve Zoho API and MongoDB credentials from Secrets Manager
def get_zoho_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Zoho credentials from Secrets Manager
zoho_credentials = get_zoho_secret("zoho_crm_credentials")
client_id = zoho_credentials['client_id']
client_secret = zoho_credentials['client_secret']
refresh_token = zoho_credentials['refresh_token']
zoho_base_url = "https://www.zohoapis.com/crm/v2/Leads"
token_url = "https://accounts.zoho.com/oauth/v2/token"

# Retrieve and refresh access token using client ID, client secret, and refresh token
def get_access_token():
    params = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token"
    }
    response = requests.post(token_url, params=params)
    response_data = response.json()
    
    if "access_token" in response_data:
        return response_data["access_token"]
    else:
        # Log error if token refresh fails
        log_error("Token refresh failed: " + response_data.get("error", "Unknown error"))
        raise Exception("Failed to retrieve access token")

zoho_api_token = get_access_token()
# Headers for Zoho CRM API
headers = {
    "Authorization": f"Zoho-oauthtoken {zoho_api_token}",
    "Content-Type": "application/json"
}

# Function to generate sample leads
def generate_sample_leads(num_leads=2500):
    leads = []
    for _ in range(num_leads):
        lead = {
            "First_Name": fake.first_name(),
            "Last_Name": fake.last_name(),
            "Email": fake.email(),
            "Phone": fake.phone_number(),
            "Company": fake.company(),
            "Lead_Status": "New"
        }
        leads.append(lead)
    return leads

# Function to upload leads to Zoho CRM
def upload_leads(leads):
    data = {
        "data": leads
    }
    response = requests.post(zoho_base_url, headers=headers, data=json.dumps(data))
    if response.status_code == 201:
        print("Leads uploaded successfully!")
    else:
        print("Failed to upload leads:", response.json())

# Generate 100 sample leads and upload them to Zoho CRM
if __name__ == "__main__":
    sample_leads = generate_sample_leads(100)
    upload_leads(sample_leads)
