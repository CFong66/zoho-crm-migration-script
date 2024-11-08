import boto3
from botocore.exceptions import ClientError

def get_and_print_secret(secret_name, region_name="ap-southeast-2"):
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
        # Handle exceptions if necessary
        raise e

    # Retrieve the secret
    secret = get_secret_value_response['SecretString']
    
    # Print the secret for testing purposes
    print(f"Secret content for {secret_name}:", secret)

# Example usage to get and print both secrets
get_and_print_secret("zoho_crm_credentials")
get_and_print_secret("zohocrmmig")
