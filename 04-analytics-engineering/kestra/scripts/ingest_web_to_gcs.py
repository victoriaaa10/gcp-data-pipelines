import os
import click
import pandas as pd
from google.cloud import storage, bigquery
from io import BytesIO

@click.command()
@click.option('--service', required=True, help="yellow, green, or fhv")
@click.option('--year', required=True, help="year of data")
@click.option('--month', required=True, type=int, help="month of data")
def ingest_data(service, year, month):
    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    project_id = os.environ.get("GCP_PROJECT_ID")
    bq_dataset = os.environ.get("BQ_DATASET_RAW")

    if not bucket_name:
        raise click.UsageError("GCS_BUCKET_NAME environment variable is missing.")
    if not project_id:
        raise click.UsageError("GCP_PROJECT_ID environment variable is missing.")
    if not bq_dataset:
        raise click.UsageError("BQ_DATASET_RAW environment variable is missing.")

    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{service}_tripdata_{year}-{month:02d}.csv.gz"
    print(f"Downloading from: {url}")

    # Process in-memory
    df = pd.read_csv(url, compression='gzip', low_memory=False)

    # Cast cols to int to avoid BigQuery schema errors
    for col in ['vendor_id', 'passenger_count', 'rate_code_id', 'payment_type']:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype('int64')

    gcs_path = f"raw/{service}/{year}/{service}_tripdata_{year}-{month:02d}.parquet"
    gcs_uri = f"gs://{bucket_name}/{gcs_path}"

    # --- Upload to GCS ---
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)

    print(f"Uploading to {gcs_uri}")
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    print("Upload complete.")

    # --- Load into BigQuery ---
    bq_client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{bq_dataset}.{service}_tripdata"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"Loading {gcs_uri} into BigQuery table {table_id}")
    load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # Wait for job to complete
    print(f"BigQuery load complete. Rows loaded: {bq_client.get_table(table_id).num_rows}")

if __name__ == "__main__":
    ingest_data()