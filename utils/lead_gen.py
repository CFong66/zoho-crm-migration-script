import csv
from faker import Faker
import random
import os

# Initialize Faker
fake = Faker()

number_of_leads = 4376

filename=f"e:/Devops/JR/P3/TF/intership/zoho/GPT/script/{number_of_leads}_leads.csv"

# Function to generate sample leads with additional fields and save to CSV
def generate_sample_leads_to_csv(samplefile, num_leads=3000):
    leads = []

    # Define some sample industries and degrees
    industries = ["Healthcare", "Education", "Finance", "Marketing", "Engineering", "Legal"]
    degrees = ["Bachelor's", "Master's", "PhD", "Diploma", "Certificate"]
    universities = ["Harvard University", "Stanford University", "University of Oxford", "MIT", "University of Cambridge"]

    # Generate the leads
    for _ in range(num_leads):
        lead = {
            "First_Name": fake.first_name(),
            "Last_Name": fake.last_name(),
            "Email": fake.email(),
            "Phone": fake.phone_number(),
            "Company": fake.company(),
            "Industry": random.choice(industries),
            "Lead_Status": "New"
        }
        leads.append(lead)
    
    # Write leads to CSV file
    with open(samplefile, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=leads[0].keys())
        writer.writeheader()
        writer.writerows(leads)

    print(f"{num_leads} leads saved to {filename}")

# Example usage
generate_sample_leads_to_csv(filename, number_of_leads)

print(f"File saved at: {os.path.abspath(filename)}")
