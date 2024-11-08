import boto3

# Initialize CloudWatch client
cloudwatch_client = boto3.client('cloudwatch', region_name='us-west-2')

def send_metrics_to_cloudwatch(metric_name, value, unit="Count"):
    cloudwatch_client.put_metric_data(
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
