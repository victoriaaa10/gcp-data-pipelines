-- Create a partitioned and clustered table
CREATE OR REPLACE TABLE `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `PROJECT_ID.nyc_yellow_taxi_2024.external_yellow_tripdata`;