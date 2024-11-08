import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import json
from datetime import datetime

# Initialize CloudWatch Logs client
cloudwatch_logs_client = boto3.client('logs', region_name='ap-southeast-2')

# Correct log group name
log_group_name = "zoho_migration"
log_stream_name = "zoho_crm_mig_2024"

# # Create the log stream if it doesn't exist
# try:
#     response = cloudwatch_logs_client.create_log_stream(
#         logGroupName=log_group_name,
#         logStreamName=log_stream_name
#     )
#     print(f"Log stream '{log_stream_name}' created successfully in log group '{log_group_name}'.")
# except cloudwatch_logs_client.exceptions.ResourceAlreadyExistsException:
#     print(f"Log stream '{log_stream_name}' already exists in log group '{log_group_name}'.")


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

# Initialize log group and stream
initialize_cloudwatch_log_group_and_stream()

# Send a test log
log_to_cloudwatch("Your test log message here new +2")