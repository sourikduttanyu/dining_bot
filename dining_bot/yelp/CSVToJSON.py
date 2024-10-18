import pandas as pd

# Load the cleaned data (you already have this step)
file_path = 'yelp_data_unique.csv'
df = pd.read_csv(file_path, delimiter='|')

# Convert the DataFrame to a JSON format
json_file_path = 'yelp_data.json'
df.to_json(json_file_path, orient='records')

# Inform the user
print(f"Data saved as JSON at {json_file_path}")
