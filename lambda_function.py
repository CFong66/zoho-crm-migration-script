
import logging
import json
import boto3
import requests
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError, ClientError
from pymongo import MongoClient
# from aws_util import log_error, send_metrics_to_cloudwatch, save_log_to_s3

# AWS clients
region_name='ap-southeast-2'
s3_client = boto3.client('s3')
cloudwatch_client = boto3.client('cloudwatch', region_name = region_name)
secrets_client = boto3.client('secretsmanager', region_name = region_name)

# Configuration
zoho_base_url = "https://www.zohoapis.com.au/crm/v2/Leads"
s3_bucket_name = "zoho-migration-cf-log"
data_discrepancies_key = "discrepancies.json"
s3_key_leads = f"leads_{datetime.now().strftime('%Y-%m-%d')}.json"
ca_lambda_bundle_path = "/tmp/global-bundle.pem"
ca_ec2_bundle_path = "/home/ubuntu/etl/zoho-crm-migration-script/etl/global-bundle.pem"
status_key = "etl_status.json"
count_discrepancies_key = "count_discrepancies.json"
cluster_identifier      = "docdb-cluster"
num_fetch_data = 4000

# Set up the logging configuration
logging.basicConfig(level=logging.INFO)

def save_log_to_s3_with_stage(stage, message, status="IN_PROGRESS"):
    log_entry = {
        "stage": stage,
        "timestamp": str(datetime.now()),
        "status": status,
        "message": message
    }
    save_log_to_s3(log_entry)


# Log Utils
def log_error(error_message, record=None):
    log_entry = {
        "timestamp": str(datetime.now()),
        "error_message": error_message,
        "record": record
    }
    logging.error(json.dumps(log_entry))
    # log_to_cloudwatch(json.dumps(log_entry))
    save_log_to_s3(log_entry)

def save_log_to_s3(log_entry):
    # Check for "error_message" key and default to an empty string if it's missing
    brief_error = log_entry.get("error_message", "").replace(" ", "_").replace("/", "_")[:50]  # Truncate to 50 characters for readability
    
    s3_key = f"logs/{datetime.now().strftime('%Y-%m-%d')}/error_{brief_error}_{datetime.now().strftime('%H-%M-%S')}.json"

    try:
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
            Body=json.dumps(log_entry),
            ContentType="application/json"
        )
    except NoCredentialsError as e:
        print("Credentials not available for S3: ", e)

# Initialize CloudWatch client
# cloudwatch_client = boto3.client('cloudwatch', region_name=region_name)

def send_metrics_to_cloudwatch(
    metric_name, 
    value, 
    unit="Count", 
    namespace="ZohoCRM_MongoDB_Migration",
    dimension_name="MigrationProject", 
    dimension_value="ZohoToMongoDB"
):
    """
    Sends a custom metric to Amazon CloudWatch.

    Parameters:
    - metric_name (str): The name of the metric.
    - value (float): The value of the metric.
    - unit (str): The unit of the metric value (e.g., "Count", "Seconds").
    - namespace (str): The CloudWatch namespace for grouping metrics.
    - dimension_name (str): The name of the metric dimension.
    - dimension_value (str): The value for the metric dimension.
    """
    try:
        response = cloudwatch_client.put_metric_data(
            region_name = region_name,
            Namespace = namespace,
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [
                        {
                            'Name': dimension_name,
                            'Value': dimension_value
                        }
                    ],
                    'Value': value,
                    'Unit': unit
                },
            ]
        )
        print(f"Metric {metric_name} sent to CloudWatch successfully:", response)
    except ClientError as e:
        print(f"Failed to send metric to CloudWatch: {e}")



# Function to fetch Zoho CRM API token
def get_zoho_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def get_access_token():

    credentials = get_zoho_secret("zoho_crm_credentials")
    token_url = f"https://accounts.zoho.com.au/oauth/v2/token?refresh_token={credentials['ZOHO_REFRESH_TOKEN']}&client_id={credentials['ZOHO_CLIENT_ID']}&client_secret={credentials['ZOHO_SECRET']}&grant_type=refresh_token"
    response = requests.post(token_url)
    return response.json()["access_token"]

# get the document db uri
def get_documentdb_uri(cluster_identifier):
    try:
        # Initialize Boto3 client for RDS (which includes DocumentDB)
        rds_client = boto3.client('rds')
        
        # Describe the DocumentDB cluster
        response = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
        
        # Extract the endpoint
        endpoint = response['DBClusters'][0]['Endpoint']
        port = response['DBClusters'][0]['Port']
        
        # Construct the MongoDB URI
        uri = f"mongodb://{endpoint}:{port}"
        
        return uri
    except ClientError as e:
        print(f"Error getting DocumentDB URI: {e}")
        return None

# Download CA certificate for MongoDB
def download_ca_certificate():
    url = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
    response = requests.get(url)
    if response.status_code == 200:
        with open(ca_lambda_bundle_path, 'wb') as f:
            f.write(response.content)
    else:
        raise Exception("Failed to download CA certificate")
    
# Check if ETL process should run
def load_etl_status_from_s3():
    try:
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=status_key)
        status = json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        status = {"run_etl": True}
    return status

def update_etl_status_in_s3(run_etl):
    status = {"run_etl": run_etl}
    s3_client.put_object(Bucket=s3_bucket_name, Key=status_key, Body=json.dumps(status))

# Check if MongoDB count matches Zoho count and log
def check_record_count():
    # MongoDB connection
    username, password, host, port, database = get_mongo_credentials()
    mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?tls=true&retryWrites=false&tlsCAFile={ca_ec2_bundle_path}"
    client = MongoClient(mongo_uri)
    db = client["zoho_crm"]
    leads_collection = db["leads"]

    # Count records in MongoDB
    mongo_count = leads_collection.count_documents({})

    # Count records in Zoho CRM
    access_token = get_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    response = requests.get(zoho_base_url, headers=headers, params={"fields": "id"})
    zoho_count = len(response.json().get("data", []))

    # Compare counts and log discrepancies if any
    log_entry = {
        "stage": "Record Count Comparison",
        "timestamp": str(datetime.now()),
        "mongo_count": mongo_count,
        "zoho_count": zoho_count,
        "status": "Match" if mongo_count == zoho_count else "Mismatch"
    }
    save_log_to_s3(log_entry)

    # If counts don't match, save discrepancies to S3 and return False
    if mongo_count != zoho_count:
        discrepancies = {"mongo_count": mongo_count, "zoho_count": zoho_count}
        s3_client.put_object(Bucket=s3_bucket_name, Key=count_discrepancies_key, Body=json.dumps(discrepancies))
        return False

    # If counts match, update status to prevent next run
    return True

# Function to send a notification (email or SMS) to the team
def send_notification(message):
    # Use Amazon SNS for notifications
    sns_client = boto3.client('sns')
    topic_arn = 'your-sns-topic-arn'  # Ensure this is set to your actual SNS topic ARN
    
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject='ETL Process Completed'
        )
        logging.info(f"Notification sent: {response}")
    except ClientError as e:
        logging.error(f"Failed to send notification: {e}")


# Fetch Zoho leads
def fetch_leads(max_records=10000):
    access_token = get_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    leads, page, per_page = [], 1, 200
    params = {"fields": "First_Name,Last_Name,Email,Phone,Company,Industry,Lead_Status", "per_page": per_page, "page": page}

    while len(leads) < max_records:
        response = requests.get(zoho_base_url, headers=headers, params=params)
        data = response.json()
        if 'data' in data:
            leads.extend(data['data'])
            send_metrics_to_cloudwatch("RecordsProcessed", len(data["data"]))
            if len(leads) >= max_records:
                leads = leads[:max_records]
                break
            page += 1
            params["page"] = page
        else:
            break

    # Save leads to S3
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key_leads, Body=json.dumps(leads))
    save_log_to_s3({"stage": "Extraction", "timestamp": str(datetime.now()), "record_count": len(leads), "status": "Data fetched"})
    
    return leads

# Get MongoDB credentials from Secrets Manager
def get_mongo_credentials():
    secret_name = "zohocrmmig"
    # region_name = "ap-southeast-2"
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret['username'], secret['password'], secret['host'], secret['port']

# Connect to MongoDB and get the leads collection
def get_leads_collection():
    # Get the DocumentDB URI
    documentdb_uri = get_documentdb_uri(cluster_identifier)
    username, password, host, port = get_mongo_credentials()

    client = MongoClient(documentdb_uri, username=username, password=password, tls=True, tlsAllowInvalidCertificates=True)
    database = "zoho_crm"
    db = client[database]
    collection = db['leads']

    return collection

# Retrieve leads from MongoDB
def get_mongo_leads():
    leads_collection = get_leads_collection()
    leads = list(leads_collection.find({}, {"_id": 0}))
    return {lead["Email"]: lead for lead in leads}

# Validate Zoho data against MongoDB
def validate_data():
    mongo_leads = get_mongo_leads()
    zoho_leads = fetch_leads(num_fetch_data)  # Fetch fresh data from Zoho

    discrepancies = []
    required_fields = ["Email", "Lead_Status", "Phone"]

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

    # Save discrepancies to S3
    if discrepancies:
        s3_client.put_object(Bucket=s3_bucket_name, Key=data_discrepancies_key, Body=json.dumps(discrepancies))
        log_entry = {
            "stage": "Validation",
            "timestamp": str(datetime.now()),
            "status": "Discrepancies found",
            "discrepancies": discrepancies
        }
        save_log_to_s3(log_entry)
    else:
        log_entry = {
            "stage": "Validation",
            "timestamp": str(datetime.now()),
            "status": "Validation successful, no discrepancies found"
        }
        save_log_to_s3(log_entry)

# Incremental load new data into MongoDB
def incremental_load(leads):
    existing_emails = get_mongo_leads().keys()
    new_leads = [lead for lead in leads if lead.get("Email") not in existing_emails]

    leads_collection = get_leads_collection()
    if new_leads:
        leads_collection.insert_many(new_leads)
        log_entry = {
            "stage": "Incremental Load",
            "timestamp": str(datetime.now()),
            "status": f"Inserted {len(new_leads)} new leads into DocumentDB"
        }
    else:
        log_entry = {
            "stage": "Incremental Load",
            "timestamp": str(datetime.now()),
            "status": "No new leads to insert"
        }
    save_log_to_s3(log_entry)

# Main ETL function
def lambda_handler(event, context):
    try:
        logging.info("Starting ETL process")
        save_log_to_s3_with_stage("ETL Start", "Starting ETL process")

        # Check ETL status
        etl_status = load_etl_status_from_s3()
        if not etl_status.get("run_etl", True):
            print("ETL job skipped due to matching record count.")
            logging.info("ETL job skipped due to matching record count.")
            save_log_to_s3_with_stage("ETL Skipped", "ETL job skipped due to matching record count.", status="SKIPPED")
            return

        # Fetch data
        logging.info("Fetching leads")
        save_log_to_s3_with_stage("Data Fetch", "Fetching leads from Zoho CRM")
        leads = fetch_leads(num_fetch_data)

        # Perform incremental load
        logging.info("Incremental load of leads")
        save_log_to_s3_with_stage("Data Load", "Performing incremental load of leads")
        incremental_load(leads)

        # Validate data
        logging.info("Validating data")
        save_log_to_s3_with_stage("Data Validation", "Validating loaded data")
        validate_data()

        # Check and compare record counts
        if check_record_count():
            update_etl_status_in_s3(run_etl=False)  # Stop ETL job if counts match
            logging.info("Record counts match; stopping ETL job.")
            save_log_to_s3_with_stage("ETL Stop", "Record counts match. ETL job stopped.", status="COMPLETED")

            # Send notification about the completion of the ETL process
            send_notification("ETL project completed successfully. MongoDB record count matches Zoho CRM.")

        else:
            update_etl_status_in_s3(run_etl=True)  # Allow ETL job to continue
            logging.info("Record counts do not match; ETL job will continue.")
            save_log_to_s3_with_stage("ETL Continue", "Record counts do not match. ETL job will continue.", status="IN_PROGRESS")

        # Final log entry for successful completion
        log_entry = {
            "stage": "ETL Completion",
            "timestamp": str(datetime.now()),
            "status": "ETL process completed successfully"
        }
        save_log_to_s3(log_entry)
    except Exception as e:
        error_log = {
            "stage": "ETL Failure",
            "timestamp": str(datetime.now()),
            "error": str(e)
        }
        save_log_to_s3(error_log)
        raise e

if __name__ == "__main__":
    lambda_handler({}, {})


