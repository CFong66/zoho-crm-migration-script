import requests
import boto3
import json
import time
from datetime import datetime
import boto3
from aws_util import log_error, send_metrics_to_cloudwatch, log_to_cloudwatch, save_log_to_s3

# log_to_cloudwatch("Starting data extraction from Zoho CRM")

# Initialize AWS Secrets Manager client
secrets_client = boto3.client('secretsmanager', region_name='ap-southeast-2')

# Retrieve Zoho API and MongoDB credentials from Secrets Manager
def get_zoho_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# # Example of getting and printing the secret content
# zoho_secret = get_zoho_secret("zoho_crm_credentials")
# print("Zoho CRM Secret:", zoho_secret)

# mongodb_secret = get_zoho_secret("zohocrmmig")
# print("MongoDB Secret:", mongodb_secret)

# Zoho credentials from Secrets Manager
print("Fetching Zoho CRM credentials...")

zoho_credentials = get_zoho_secret("zoho_crm_credentials")
client_id = zoho_credentials['ZOHO_CLIENT_ID']
client_secret = zoho_credentials['ZOHO_SECRET']
refresh_token = zoho_credentials['ZOHO_REFRESH_TOKEN']
zoho_base_url = "https://www.zohoapis.com.au/crm/v2/Leads"

# fields=First_Name,Last_Name,Email,Phone,Company,Industry,Lead_Status??per_page=20&page=1
print("Zoho CRM credentials retrieved successfully.")
Accounts_URL= "https://accounts.zoho.com.au"

# token_url = "https://accounts.zoho.com.au/oauth/v2/token"
token_url = f'{Accounts_URL}/oauth/v2/token?refresh_token={refresh_token}&client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token'

# Retrieve and refresh access token using client ID, client secret, and refresh token
def get_access_token():
    print("Refreshing access token...")
    # params = {
    #     "refresh_token": refresh_token,
    #     "client_id": client_id,
    #     "client_secret": client_secret,
    #     "grant_type": "refresh_token"
    # , params=params
    # }
    response = requests.post(token_url)
    response_data = response.json()

    # Debug response to check for errors
    print("Zoho Token Response:", response_data)
    
    if "access_token" in response_data:
        print("Access token retrieved successfully.")
        return response_data["access_token"]
    else:
        # Log error if token refresh fails
        log_error("Token refresh failed: " + response_data.get("error", "Unknown error"))
        raise Exception("Failed to retrieve access token")

# Function to fetch leads from Zoho CRM
def fetch_leads(max_records=1500):
    print("Fetching leads from Zoho CRM...")
    zoho_api_token = get_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {zoho_api_token}"}
    zoho_base_url = "https://www.zohoapis.com.au/crm/v2/Leads"
    leads = []
    page = 1
    per_page = 200  # Number of records per page
    params = {
        "fields": "First_Name,Last_Name,Email,Phone,Company,Industry,Lead_Status",
        "per_page": per_page,
        "page": page
    }

    while len(leads) < max_records:
        print(f"Fetching page {page} of leads...")
        try:
            response = requests.get(zoho_base_url, headers=headers, params=params)
            data = response.json()

            # Debug response to check for data
            print("API Response:", data)
            
            # Check if 'data' exists in the response
            if 'data' in data:
                leads.extend(data['data'])
                print(f"Retrieved {len(data['data'])} leads from page {page}.")
                # send_metrics_to_cloudwatch("RecordsProcessed", len(data["data"]))

                # Check if we've reached the max_records
                if len(leads) >= max_records:
                    leads = leads[:max_records]  # Trim to the exact max_records
                    break

                page += 1
                params["page"] = page  # Update the page parameter
            else:
                print("No more leads to fetch.")
                break
        except Exception as e:
            # Log error and mark as failed in CloudWatch
            # log_to_cloudwatch(f"Error fetching leads on page {page}: {str(e)}")
            # send_metrics_to_cloudwatch("FailedRecords", 1)
            log_error(str(e), record=page)
            break

    print(f"Total leads fetched: {len(leads)}")

    log_entry = {
        "stage": "Extraction",
        "timestamp": str(datetime.now()),
        "record_count": len(leads),
        "status": "Data fetched from Zoho CRM"
    }
    save_log_to_s3(log_entry)

    return leads




# # Fetch and print leads
# if __name__ == "__main__":
#     leads_data = fetch_leads()
#     with open("leads_data.json", "w") as file:
#         json.dump(leads_data, file)
#     print("Leads data extracted and saved to leads_data.json.")


if __name__ == "__main__":
    leads_data = fetch_leads(max_records=200)
    if leads_data:  # Check if data is not empty before saving
        with open("/home/ubuntu/etl/zoho-etl-script/etl/leads_data.json", "w") as file:
            json.dump(leads_data, file)
        print("Leads data extracted and saved to leads_data.json.")
    else:
        print("No leads data to save.")
