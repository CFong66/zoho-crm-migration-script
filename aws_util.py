import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import json
from datetime import datetime

# CloudWatch log configuration
cloudwatch_logs_client = boto3.client('logs', region_name='ap-southeast-2')
log_group_name = "zoho_migration"
log_stream_name = "zoho_crm_mig_2024"

# S3 bucket for long-term log storage
s3_client = boto3.client('s3')
s3_bucket_name = "zoho-mig-mgdb-cf-log"

def setup_logging():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    return logger

def initialize_cloudwatch_log_group_and_stream():
    try:
        cloudwatch_logs_client.create_log_group(logGroupName=log_group_name)
        print(f"Log group '{log_group_name}' created successfully.")
    except cloudwatch_logs_client.exceptions.ResourceAlreadyExistsException:
        print(f"Log group '{log_group_name}' already exists.")

    existing_streams = cloudwatch_logs_client.describe_log_streams(
        logGroupName=log_group_name,
        logStreamNamePrefix=log_stream_name
    )['logStreams']
    
    if not existing_streams:
        cloudwatch_logs_client.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
        print(f"Log stream '{log_stream_name}' created successfully in log group '{log_group_name}'.")
    else:
        print(f"Log stream '{log_stream_name}' already exists in log group '{log_group_name}'.")

def log_to_cloudwatch(message):
    try:
        response = cloudwatch_logs_client.describe_log_streams(
            logGroupName=log_group_name,
            logStreamNamePrefix=log_stream_name
        )
        log_streams = response['logStreams']
        if not log_streams:
            print(f"No log streams found in log group '{log_group_name}' for '{log_stream_name}'")
            return

        sequence_token = log_streams[0].get('uploadSequenceToken')
        log_event = {
            'logGroupName': log_group_name,
            'logStreamName': log_stream_name,
            'logEvents': [
                {
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'message': message
                }
            ]
        }
        if sequence_token:
            log_event['sequenceToken'] = sequence_token

        response = cloudwatch_logs_client.put_log_events(**log_event)
        print("Log event sent to CloudWatch:", response)
    except Exception as e:
        print(f"Failed to log to CloudWatch: {e}")

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
    # Shorten and sanitize the error message for S3 filename compatibility
    brief_error = log_entry["error_message"].replace(" ", "_").replace("/", "_")[:50]  # Truncate to 50 characters for readability
    
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
cloudwatch_client = boto3.client('cloudwatch', region_name='ap-southeast-2')

def send_metrics_to_cloudwatch(metric_name, value, unit="Count"):
    try:
        response = cloudwatch_client.put_metric_data(
            Namespace='ZohoCRM_MongoDB_Migration',
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [
                        {
                            'Name': 'MigrationProject',
                            'Value': 'ZohoToMongoDB'
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
