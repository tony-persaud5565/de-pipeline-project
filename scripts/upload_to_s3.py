import boto3
from botocore.exceptions import NoCredentialsError
import os

# Set these values
BUCKET_NAME = 'de-project-bucket5565'  
FILE_PATH = os.path.abspath('../data/fakestore_products.csv')
KEY = 'raw/fakestore_products.csv'

def upload_file():
    s3 = boto3.client('s3')  

    try:
        s3.upload_file(FILE_PATH, BUCKET_NAME, KEY)
        print(f"✅ Uploaded {FILE_PATH} to s3://{BUCKET_NAME}/{KEY}")
    except FileNotFoundError:
        print("❌ File not found.")
    except NoCredentialsError:
        print("❌ AWS credentials not found. Did you run aws configure?")
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    upload_file()