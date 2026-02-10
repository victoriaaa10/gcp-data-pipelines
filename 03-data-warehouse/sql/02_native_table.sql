-- Create a non-partitioned table from the external table
CREATE OR REPLACE TABLE `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_non_partitioned` AS
SELECT * FROM `PROJECT_ID.nyc_yellow_taxi_2024.external_yellow_tripdata`;