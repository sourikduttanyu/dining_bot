from opensearchpy import OpenSearch, RequestsHttpConnection
from requests.auth import HTTPBasicAuth
import json

# Initialize OpenSearch client with your domain and credentials
client = OpenSearch(
    hosts=[{'host': 'search-yelp-opensearch-4bdykukdzy2ztbemqqjjzqkiym.us-west-2.es.amazonaws.com', 'port': 443}],
    http_auth=HTTPBasicAuth('yelp-master', 'Yelp-Password1'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

index_name = 'restaurants'  # Ensure the index matches the one you created

# Load Yelp data from a JSON file
with open('yelp_data.json') as f:
    yelp_data = json.load(f)

# Function to convert coordinates from string to object format
def format_coordinates(coordinate_string):
    try:
        lat, lon = map(float, coordinate_string.split(','))
        return {"lat": lat, "lon": lon}
    except (ValueError, AttributeError) as e:
        print(f"Invalid coordinates: {coordinate_string}. Skipping this entry.")
        return None

# Index each restaurant entry in the Yelp data
for restaurant in yelp_data:
    business_id = restaurant['business_id']
    
    # Convert the coordinates to the correct format
    coordinates = format_coordinates(restaurant['coordinates'])
    
    # Skip indexing if the coordinates are invalid
    if not coordinates:
        continue
    
    # Update the coordinates in the restaurant data
    restaurant['coordinates'] = coordinates
    
    # Index the document using the business_id as the unique document ID
    response = client.index(
        index=index_name,
        id=business_id,
        body=restaurant
    )
    print(f"Indexed {restaurant['name']} with ID {business_id}")
