# Week 2: Workflow Orchestration with Kestra


### Content covered
- What is workflow orchestration? What is Kestra?
- Getting started with Kestra: Installation, adding Flows to Kestra, and Kestra Concepts
- Building ETL Data pipelines for Yellow/Green NYC Taxi Data
    - extracting data from CSV files
    - loading taxi data into local Postgres database, and Google Cloud using BCS + BigQuery
    - scheduling and backfills using the Kestra UI



### Homework - Quiz Questions

**Q1.** Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
- A. 128.3 MiB
- B. 134.5 MiB
- C. 364.7 MiB
- D. 692.6 MiB

I created a size task within the flow that generates an output with the size of the file in bytes. I then converted this to MiB.

**Answer**: A


**Q2.** What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
- A. {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
- B. green_tripdata_2020-04.csv
- C. green_tripdata_04_2020.csv
- D. green_tripdata_2020.csv

The file variable in our Kestra flow is "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"

Setting inputs.taxi = green, inputs.year = 2020 and inputs.month to 04 will render to
green_tripdata_2020-04.csv

**Answer**: B


**Q3.** How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
- A. 13,537.299
- B. 24,648,499
- C. 18,324,219
- D. 29,430,127

Using the schedule trigger in the Kestra UI, I backfilled all Yellow Taxi data for 2020 into my local Postgres database, and
used the following query to find total rows for the above data.

```SQL
SELECT COUNT(*) FROM yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2020%';
```

**Answer**: B

**Q4.** How many rows are there for the Green Taxi data for all CSV files in the year 2020?
- A.5,327,301
- B.936,199
- C.1,734,051
- D. 1,342,034

Followed the same steps as Q4 to load the Green Taxi data for 2020, then queried row count with the following query.

```SQL
SELECT COUNT(*) FROM green_tripdata
WHERE filename LIKE 'green_tripdata_2020%';
```
**Answer**: C

**Q5.** How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
- A. 1,428,092
- B. 706,911
- C. 1,925,152
- D. 2,561,031

Same approach as Q4 and 5.

```SQL
SELECT COUNT(*) FROM yellow_tripdata
WHERE filename LIKE 'green_tripdata_2020%'
```
**Answer**: C

**Q6**: How would you configure the timezone to New York in a Schedule trigger?

- A. Add a timezone property set to EST in the Schedule trigger configuration
- B. Add a timezone property set to America/New_York in the Schedule trigger configuration
- C. Add a timezone property set to UTC-5 in the Schedule trigger configuration
- D. Add a location property set to New_York in the Schedule trigger configuration

Referred to Kestra documentation https://kestra.io/docs/workflow-components/triggers/schedule-trigger

**Answer**: B
