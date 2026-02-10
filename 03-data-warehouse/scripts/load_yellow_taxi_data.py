"""
Script: load_yellow_taxi_data.py
Description: Downloads 2024 Yellow Taxi data and uploads it to Google Cloud Storage (GCS).
Course: DataTalksClub Data Engineering Zoomcamp (2026)

Credits: 
- Original script logic provided by DataTalksClub.
- Modified by: Victoria T.

Key Modifications:
- Configuration: Integrated hcl2 to dynamically sync with terraform.tfvars.
- Auth: Added automated fallback between Service Account JSON and ADC.
- Efficiency: Streamlined bucket verification to reduce redundant API calls.
- Security: Redacted hardcoded Project IDs and Bucket names from source code.
"""

import os
import sys
import urllib.request
import time
import hcl2
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

# --- Configuration Loading ---
def load_config(tfvars_path="../terraform/terraform.tfvars"):
    """Parses terraform.tfvars to redact sensitive info from the script."""
    try:
        with open(tfvars_path, 'r') as file:
            config = hcl2.load(file)
            # hcl2 loads values as lists, extract the first element
            return {k: v[0] if isinstance(v, list) else v for k, v in config.items()}
    except FileNotFoundError:
        print(f"Warning: {tfvars_path} not found. Falling back to defaults.")
        return {}

# Load variables from TF
TF_CONFIG = load_config()

BUCKET_NAME = TF_CONFIG.get("gcs_bucket_name", "bucket-name")
PROJECT_ID  = TF_CONFIG.get("project", "project-id")
CREDENTIALS_FILE = "gcs.json"

# Initialize GCS Client
if os.path.exists(CREDENTIALS_FILE):
    print(f"Using Service Account Key: {CREDENTIALS_FILE}")
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
else:
    print(f"No JSON key found. Using Application Default Credentials (ADC)...")
    client = storage.Client(project=PROJECT_ID)

# --- Script Logic ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # gets the location of the script DOWNLOAD_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data")) # moves up and into /data

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

# create the data directory if it doesn't exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)

def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try: 
        # check if file already exists to avoid redundant downloads 
        if os.path.exists(file_path): 
            print(f"File already exists: {file_path}. Skipping download.") 
            return file_path 

        print(f"Downloading {url}...") 
        urllib.request.urlretrieve(url, file_path) 
        print(f"Downloaded: {file_path}") 
        return file_path 
    except Exception as e: 
        print(f"Failed to download {url}: {e}") 
        return None

def create_bucket(bucket_name):
    try:
        client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' verified.")
    except NotFound:
        print(f"Creating bucket '{bucket_name}'...")
        client.create_bucket(bucket_name, location="US")
    except Forbidden:
        print(f"Access Denied: Bucket name '{bucket_name}' is taken. Please try a different bucket name.")
        sys.exit(1)

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Failed to upload {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")