from opensearchpy import OpenSearch, RequestsHttpConnection
from requests.auth import HTTPBasicAuth

# Initialize OpenSearch client with your domain and credentials
client = OpenSearch(
    hosts=[{'host': 'search-yelp-opensearch-4bdykukdzy2ztbemqqjjzqkiym.us-west-2.es.amazonaws.com', 'port': 443}],
    http_auth=HTTPBasicAuth('yelp-master', 'Yelp-Password1'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

index_name = 'restaurants'  # Change the index name to 'restaurants'
index_body = {
    'settings': {
        'index': {
            'number_of_shards': 1,    # Customize the number of shards based on your requirements
            'number_of_replicas': 1   # Customize the number of replicas
        }
    },
    'mappings': {
        'properties': {
            'business_id': {'type': 'keyword'},       # Unique business identifier
            'name': {'type': 'text'},                # Restaurant name
            'address': {'type': 'text'},             # Restaurant address
            'coordinates': {'type': 'geo_point'},    # Geo-coordinates for the restaurant
            'number_of_reviews': {'type': 'integer'},# Number of reviews
            'rating': {'type': 'float'},             # Rating (float value)
            'zip_code': {'type': 'keyword'},         # Zip code
            'cuisine': {'type': 'keyword'}           # Cuisine type (e.g., Italian, Indian)
        }
    }
}

# Create the index with the defined settings and mappings
response = client.indices.create(index=index_name, body=index_body)
print(response)
