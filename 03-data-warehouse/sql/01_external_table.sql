-- Create an external table pointing to the 2024 Yellow Taxi Parquet files
CREATE OR REPLACE EXTERNAL TABLE `PROJECT_ID.nyc_yellow_taxi_2024.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://BUCKET_NAME/yellow_tripdata_2024-*.parquet']
);