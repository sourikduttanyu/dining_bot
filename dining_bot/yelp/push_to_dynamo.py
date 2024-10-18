import boto3
import json
from decimal import Decimal
from botocore.exceptions import ClientError

# Initialize a Boto3 DynamoDB resource
dynamodb = boto3.resource('dynamodb',aws_access_key_id='AKIAW5WU5I4P3GH5YJ5V', aws_secret_access_key='OyO6jae+ujoC3ch8Gy3x3CUzubqGPw4I8CixzH0q',region_name='us-west-2')

# Specify your DynamoDB table name
table_name = 'yelp-restaurants'
table = dynamodb.Table(table_name)

# Helper function to convert float values to Decimal
def convert_floats_to_decimal(data):
    if isinstance(data, list):
        return [convert_floats_to_decimal(i) for i in data]
    elif isinstance(data, dict):
        return {k: convert_floats_to_decimal(v) for k, v in data.items()}
    elif isinstance(data, float):
        return Decimal(str(data))
    return data

def load_data_to_dynamodb(filename):
    # Open and read the JSON file
    with open('yelp_data.json') as file:
        items = json.load(file, parse_float=Decimal)  # Convert all float numbers to Decimal automatically
        
    # Iterate over items and put each into the DynamoDB table
    for item in items:
        try:
            print(f"Adding item: {item['business_id']}")
            # Put the item into the DynamoDB table
            response = table.put_item(Item=convert_floats_to_decimal(item))
            print(f"Item added: {item['business_id']}")
        except ClientError as e:
            print(f"Error adding item {item['business_id']}: {e.response['Error']['Message']}")

# Replace 'yelp_data.json' with the path to your JSON file
load_data_to_dynamodb('yelp_data_unique.json')
