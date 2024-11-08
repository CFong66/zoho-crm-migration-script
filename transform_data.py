import json
from aws_util import log_error, send_metrics_to_cloudwatch, log_to_cloudwatch, save_log_to_s3
from datetime import datetime


# log_to_cloudwatch("Starting data transformation from Zoho CRM")

# Transform function with validation for required fields
def transform_lead_data(lead):
    transformed = {k: v for k, v in lead.items() if v}
    required_fields = ["Email", "Lead_Status", "Phone"]

    for field in required_fields:
        if field not in transformed:
            transformed[field] = None  # Default value if missing
            print(f"Warning: Field '{field}' missing in record. Added as None.")

    return transformed

def transform_data():
    try:
        with open("/home/ubuntu/etl/zoho-etl-script/etl/leads_data.json", "r") as file:
            leads_data = json.load(file)

        transformed_leads = []
        
        for index, lead in enumerate(leads_data):
            try:
                transformed = transform_lead_data(lead)
                transformed_leads.append(transformed)

                # send_metrics_to_cloudwatch("RecordsProcessed", 1)  # Send metric on success
            except Exception as e:
                # send_metrics_to_cloudwatch("FailedRecords", 1)
                log_error(str(e), record=index)  # Log with index as identifier

        # Save transformed data for loading
        with open("/home/ubuntu/etl/zoho-etl-script/etl/transformed_leads.json", "w") as file:
            json.dump(transformed_leads, file)

        print("Data transformation complete. Transformed data saved to transformed_leads.json.")

    except Exception as e:
        # Catch errors that might occur in the outer function scope
        # send_metrics_to_cloudwatch("FailedRecords", 1)
        log_error(str(e), record="general")

if __name__ == "__main__":
    transform_data()
