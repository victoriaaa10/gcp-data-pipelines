import os
import click
import pandas as pd
from google.cloud import storage
from io import BytesIO

@click.command()
@click.option('--service', required=True, help="taxi service (yellow, green, fhv)")
@click.option('--year', required=True, help="year of data")
@click.option('--month', required=True, type=int, help="month of data")
def fetch_and_process(service, year, month):
    """Ingest NYC Taxi data to GCS."""
    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    if not bucket_name:
        raise click.UsageError("Environment variable GCS_BUCKET_NAME is missing.")

    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{service}_tripdata_{year}-{month:02d}.csv.gz"
    print(f"Fetching: {url}")

    # Data Processing Logic
    df = pd.read_csv(url, compression='gzip', low_memory=False)
    
    # Clean schema
    cols_to_fix = ['vendor_id', 'passenger_count', 'rate_code_id', 'payment_type']
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype('int64')

    # GCS Upload Logic
    gcs_path = f"raw/{service}/{year}/{service}_tripdata_{year}-{month:02d}.parquet"
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)

    print(f"Uploading to gs://{bucket_name}/{gcs_path}")
    blob.upload_from_file(buffer, content_type='application/octet-stream')

if __name__ == "__main__":
    fetch_and_process()