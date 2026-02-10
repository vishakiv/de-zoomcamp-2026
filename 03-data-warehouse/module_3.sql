-- Query publicly available citibike station table
SELECT station_id, name 
FROM
    bigquery-public-data.new_york.citibike_stations
LIMIT 100;

-- Creating an external table which refers to gcs path
-- created an external table "external_yellow_tripdata_19_20" in the dataset "zoomcamp" within my project_id "datatalks-de-course-485418"
-- table reads CSV files from the location(uri) of the file in GCS Bucket
CREATE OR REPLACE EXTERNAL TABLE `datatalks-de-course-485418.zoomcamp.external_yellow_tripdata_19_20`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-zoomcamp-vishaki-demo/yellow_tripdata_2019-*.csv', 'gs://kestra-zoomcamp-vishaki-demo/yellow_tripdata_2020-*.csv']
);

-- Having a quick look at the data
SELECT *
FROM datatalks-de-course-485418.zoomcamp.external_yellow_tripdata_19_20
LIMIT 10;

-- Now let's create a non-partitioned regular table from the external table
CREATE OR REPLACE TABLE datatalks-de-course-485418.zoomcamp.yellow_tripdata_19_20_non_partitioned AS
SELECT * FROM datatalks-de-course-485418.zoomcamp.external_yellow_tripdata_19_20;

-- Create a partitioned table from the external table
CREATE OR REPLACE TABLE datatalks-de-course-485418.zoomcamp.yellow_tripdata_19_20_partitioned 
PARTITION BY DATE(tpep_pickup_datetime) AS
SELECT * 
FROM datatalks-de-course-485418.zoomcamp.external_yellow_tripdata_19_20;


---- Impact of partition on the amount of data processed
-- Non-partitioned table scans 1.62GB of data
SELECT DISTINCT(VendorID)
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_19_20_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Partitioned table scans ~106 MB of data
SELECT DISTINCT(VendorID)
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_19_20_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

--- Looking into the partitions
-- allows us to see how many rows fall into each partition
SELECT table_name, partition_id, total_rows
FROM `zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_19_20_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE datatalks-de-course-485418.zoomcamp.yellow_tripdata_19_20_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM datatalks-de-course-485418.zoomcamp.external_yellow_tripdata_19_20;

---Comparing processing in a partitioned table vs partitioned and clustered
-- Query scans 1.06 GB
SELECT count(*) as trips
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_19_20_partitioned 
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 844 MB -> slight performance improvement
SELECT count(*) as trips
FROM datatalks-de-course-485418.zoomcamp.yellow_tripdata_19_20_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

