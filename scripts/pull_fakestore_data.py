import requests
import pandas as pd
import os

API_URL = 'https://fakestoreapi.com/products'
JSON_PATH = os.path.abspath('../data/fakestore_products.json')
CSV_PATH = os.path.abspath('../data/fakestore_products.csv')

def fetch_and_save():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # Save raw JSON
        with open(JSON_PATH, 'w') as f:
            import json
            json.dump(data, f, indent=2)

        # Convert to CSV using pandas
        df = pd.json_normalize(data)
        df.to_csv(CSV_PATH, index=False)

        print(f"Saved {len(data)} products to:")
        print(f" - JSON: {JSON_PATH}")
        print(f" - CSV : {CSV_PATH}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_and_save()
