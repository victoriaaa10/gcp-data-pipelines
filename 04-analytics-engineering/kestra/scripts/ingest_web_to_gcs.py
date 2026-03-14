import os
import click
import pandas as pd
from google.cloud import storage
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

@click.command()
@click.option('--service', required=True, help="yellow, green, or fhv")
@click.option('--year', required=True, help="year of data")
@click.option('--month', required=True, type=int, help="month of data")
def ingest_data(service, year, month):
    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    if not bucket_name:
        raise click.UsageError("Missing required environment variable GCS_BUCKET_NAME.")

    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{service}_tripdata_{year}-{month:02d}.csv.gz"
    print(f"Downloading from: {url}")

    int_cols = [
        'vendorid', 'vendor_id',
        'passenger_count',
        'ratecodeid', 'rate_code_id',
        'payment_type',
        'trip_type',        
        'pulocationid',    
        'dolocationid',     
    ]

    buffer = BytesIO()
    writer = None
    schema = None
    total_rows = 0

    for chunk in pd.read_csv(url, compression='gzip', low_memory=False, chunksize=100_000):
        # Standardize column names
        chunk.columns = [c.lower() for c in chunk.columns]

        # Cast int columns
        for col in int_cols:
            if col in chunk.columns:
                chunk[col] = chunk[col].fillna(0).astype('int64')

        # Infer schema from first chunk and enforce on all subsequent chunks
        if schema is None:
            # fill nulls
            chunk = chunk.fillna({
                col: 0 for col in chunk.select_dtypes(include='number').columns
            })
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            schema = table.schema
            writer = pq.ParquetWriter(buffer, schema)
        else:
            # cast chunk to match the established schema
            table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False, safe=False)

        writer.write_table(table)
        total_rows += len(chunk)
        print(f"Processed {total_rows:,} rows...")

    if writer:
        writer.close()

    buffer.seek(0)

    gcs_path = f"raw/{service}/{year}/{service}_tripdata_{year}-{month:02d}.parquet"
    gcs_uri = f"gs://{bucket_name}/{gcs_path}"

    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    print(f"Uploading {total_rows:,} rows to {gcs_uri}")
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    print(f"Successfully uploaded to GCS: {gcs_uri}")

if __name__ == "__main__":
    ingest_data()