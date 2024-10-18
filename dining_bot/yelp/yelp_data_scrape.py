import requests
import csv
import datetime

# Your Yelp API Key
API_KEY = "1wXJjNTqYvEaeSOFVfz60a48AisOlmIBp8MCVROSu11mGFLEYctdmCFDOKZ8kaey8X-cXDW6yq0UiCRjHPoZ8wK0ZH5ouiy-zotkCP-MrNMJpMrUSXShslC767QRZ3Yx"

# API constants
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
SEARCH_LIMIT = 50  # Max limit allowed by Yelp per request

# Function to perform a search using Yelp API
def search(api_key, term, location, offset):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT,
        'offset': offset  # Offset for pagination
    }
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }
    url = f'{API_HOST}{SEARCH_PATH}'
    response = requests.get(url, headers=headers, params=url_params)

    return response.json()

# Function to scrape Yelp data for different cuisines and save to CSV
def scrape_yelp():
    # Define the cuisines and location
    cuisines = ['Chinese', 'Italian', 'Indian','Mexican','Japanese','French']  # Add more cuisines if needed
    location = 'Manhattan'

    # Open CSV file for appending data
    with open('yelp_data_new.csv', 'a', newline='', encoding='utf-8') as yelp_csv:
        field_names = ['business_id', 'insertedAtTimestamp', 'name', 'address', 'coordinates', 'number_of_reviews', 'rating', 'zip_code', 'cuisine']
        writer = csv.DictWriter(yelp_csv, fieldnames=field_names, delimiter='|')
        
        # Write header (only the first time you run this script, comment it out after first run)
        writer.writeheader()

        # Loop through each cuisine
        for cuisine in cuisines:
            print(f"Scraping {cuisine} cuisine...")
            cuisine_term = cuisine + ' restaurant'
            offset = 0
            total_scraped = 0
            res_id = set()  # Using a set to avoid duplicates

            # Continue paginating until we get 1,000 restaurants
            while total_scraped < 1000:
                print(f"Fetching batch with offset {offset} for {cuisine_term}")
                res = search(API_KEY, cuisine_term, location, offset)

                if 'businesses' not in res or len(res['businesses']) == 0:
                    print(f"No more results found for {cuisine_term}.")
                    break

                # Loop through the results and process each business
                for business in res['businesses']:
                    if business['id'] in res_id:  # Skip if the business is already processed
                        continue

                    res_id.add(business['id'])  # Track unique business IDs
                    row = {
                        'business_id': business['id'],
                        'insertedAtTimestamp': str(datetime.datetime.now()),
                        'name': business['name'],
                        'address': ', '.join(business['location']['display_address']),
                        'coordinates': f"{business['coordinates']['latitude']},{business['coordinates']['longitude']}",
                        'number_of_reviews': business['review_count'],
                        'rating': business['rating'],
                        'zip_code': business['location'].get('zip_code', 'N/A'),
                        'cuisine': cuisine  # Record the cuisine type
                    }

                    # Write the row to the CSV file
                    writer.writerow(row)
                    total_scraped += 1

                    # Stop once we reach 1,000 records for this cuisine
                    if total_scraped >= 1000:
                        print(f"Reached 1,000 records for {cuisine_term}.")
                        break

                # Update offset to fetch the next set of results
                offset += SEARCH_LIMIT
                print(f"Total {cuisine_term} restaurants scraped so far: {total_scraped}")

            print(f"Finished scraping {cuisine}. Total records: {total_scraped}")

# Main function to initiate the scraping
def main():
    scrape_yelp()

# Execute the main function
if __name__ == '__main__':
    main()
