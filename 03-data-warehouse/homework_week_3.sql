
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `datatalks-de-course-485418.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-vishaki-demo/yellow_tripdata_2024-*.parquet']
);

-- Creating a regular table
CREATE OR REPLACE TABLE datatalks-de-course-485418.zoomcamp.yellow_tripdata_regular
AS
SELECT *
FROM datatalks-de-course-485418.zoomcamp.external_yellow_tripdata;


-- Counting records for 2024 Taxi Data (Jan-June)
SELECT COUNT(*)
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_regular;
--result: 20332093

-- Counting distinct number of PULocationIDs for regular table
SELECT COUNT(DISTINCT(PULocationID))
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_regular;
-- estimated query size 155.12 MB

-- Counting distinct number of PULocationIDs for external table
SELECT COUNT(DISTINCT(PULocationID))
FROM datatalks-de-course-485418.zoomcamp.external_yellow_tripdata;
-- estimated query size O B

--Retrieve PULocationID from the regular table
SELECT PULocationID
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_regular;
-- estimated query size 155.12 MB

--Retrieve PULocationID and DOLocationID from the regular table 
SELECT PULocationID, DOLocationID
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_regular;
-- estimated query size 310.24 MB

-- Counting zero-fare trips
SELECT COUNT(*)
FROM datatalks-de-course-485418.zoomcamp.external_yellow_tripdata
WHERE fare_amount = 0;
-- result: 8333

-- Partitioning by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE datatalks-de-course-485418.zoomcamp.yellow_tripdata_partitioned
PARTITION BY 
  DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM datatalks-de-course-485418.zoomcamp.external_yellow_tripdata;


--Retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) 
-- regular table
SELECT DISTINCT(VendorID)
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_regular
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';
-- estimated query size for regular table: 310.24 MB

-- partitioned table
SELECT DISTINCT(VendorID)
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_partitioned
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';
-- estimated query size for regular table: 26.84 MB


