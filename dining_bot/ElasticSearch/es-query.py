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

# Define the index name
index_name = 'restaurants'

# Construct the query body to retrieve all documents
query_body = {
    "query": {
        "match_all": {}
    }
}

# Perform the search query
response = client.search(body=query_body, index=index_name)

# Print all results
for hit in response['hits']['hits']:
    print(f"Name: {hit['_source']['name']}, Address: {hit['_source']['address']}, Rating: {hit['_source']['rating']}, Cuisine: {hit['_source']['cuisine']}")
