-- Module 3: Data Warehousing Homework 
-- 2024 Yellow Taxi Data (Jan - June)

-------------------------------------------------------------------------
-- SETUP 
-------------------------------------------------------------------------

-- Create External Table pointing to GCS
CREATE OR REPLACE EXTERNAL TABLE `PROJECT_ID.nyc_yellow_taxi_2024.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://BUCKET_NAME/yellow_tripdata_2024-*.parquet']
);

-- Create Native (Materialized) Table
CREATE OR REPLACE TABLE `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_non_partitioned` AS
SELECT * FROM `PROJECT_ID.nyc_yellow_taxi_2024.external_yellow_tripdata`;


-------------------------------------------------------------------------
-- QUESTION 1: What is count of records for the 2024 Yellow Taxi Data?
-------------------------------------------------------------------------
SELECT count(*) AS total_records 
FROM `PROJECT_ID.nyc_yellow_taxi_2024.external_yellow_tripdata`;
-- Result: 20,332,093


-------------------------------------------------------------------------
-- QUESTION 2: Data read estimation (External vs. Native)
-------------------------------------------------------------------------

-- For External Table 
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `PROJECT_ID.nyc_yellow_taxi_2024.external_yellow_tripdata`;
-- Result: 0 MB 
-- BigQuery does not store metadata for external files

-- For Native Table
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_non_partitioned`;
-- Result: 155.12 MB


-------------------------------------------------------------------------
-- QUESTION 3: Why are the estimated number of Bytes different?
-------------------------------------------------------------------------
-- Scenario: SELECT PULocationID vs SELECT PULocationID, DOLocationID
-- Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. 
-- Querying two columns requires reading more data than querying one column.


-------------------------------------------------------------------------
-- QUESTION 4: How many records have a fare_amount of 0?
-------------------------------------------------------------------------
SELECT count(*) 
FROM `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;
-- Result: 8,333


-------------------------------------------------------------------------
-- QUESTION 5: Best strategy for optimization
-------------------------------------------------------------------------
-- Goal: Filter by tpep_dropoff_datetime and order by VendorID
-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

CREATE OR REPLACE TABLE `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `PROJECT_ID.nyc_yellow_taxi_2024.external_yellow_tripdata`;


-------------------------------------------------------------------------
-- QUESTION 6: Performance Scan Comparison
-------------------------------------------------------------------------

-- Materialized (Non-partitioned) table scan
SELECT DISTINCT(VendorID)
FROM `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Estimated Bytes Processed: ~310.24 MB

-- Partitioned table scan
SELECT DISTINCT(VendorID)
FROM `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Estimated Bytes Processed: ~26.84 MB


-------------------------------------------------------------------------
-- QUESTION 7: Where is the data stored in the External Table?
-------------------------------------------------------------------------
-- Answer: GCP Bucket


-------------------------------------------------------------------------
-- QUESTION 8: Is it best practice to always cluster your data?
-------------------------------------------------------------------------
-- Answer: False 
-- Clustering only helps with tables > 1GB or specific high-cardinality filters.


-------------------------------------------------------------------------
-- QUESTION 9: SELECT count(*) on a Materialized Table
-------------------------------------------------------------------------
SELECT count(*) 
FROM `PROJECT_ID.nyc_yellow_taxi_2024.yellow_tripdata_non_partitioned`;

-- Estimated Bytes: 0 B
-- Why? 
-- BigQuery stores the total row count for materialized tables in its metadata. 
-- Since it can answer this specific query by just looking at the table's 
-- metadata without scanning any actual data rows, the estimated bytes read is 0.