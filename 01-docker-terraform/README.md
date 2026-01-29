# Week 1: Introduction to Docker and Terraform, SQL refresher


### Contents


### Homework

Q1. Understanding Docker Images
Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container. What's the version of pip in the image?

Answer: 25.3 
Shell command
```bash
docker run -it --rm --entrypoint=bash python:3.13
pip --version
```

Q3. Counting Short Trips
For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

```SQL
SELECT
  COUNT(*)
FROM green_taxi_data
WHERE lpep_pickup_datetime >='2025-11-01' AND
	    lpep_pickup_datetime < '2025-12-01' AND
	     trip_distance <= 1.0;
```


Q4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).
Use the pick up time for your calculations.

```SQL
SELECT
  CAST(lpep_pickup_datetime AS DATE) AS "day",
  MAX(trip_distance)
FROM green_taxi_data
GROUP BY "day"
HAVING MAX(trip_distance) < 100.0
ORDER BY MAX(trip_distance) DESC;
```


Q5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

```SQL
SELECT
   zpu."Zone" AS pickup_zone,
   SUM(total_amount)
FROM green_taxi_data t
JOIN zones zpu
    ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo
    ON t."DOLocationID" = zdo."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-18'
  AND t.lpep_pickup_datetime <  '2025-11-19'
GROUP BY pickup_zone
ORDER BY SUM(total_amount) DESC
LIMIT 1;
```

Q6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?
Note: it's tip , not trip. We need the name of the zone, not the ID.

```SQL
SELECT
   zdo."Zone" AS dropoff_zone,
   MAX(tip_amount)
FROM green_taxi_data t
JOIN zones zpu
    ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo
    ON t."DOLocationID" = zdo."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime < '2025-12-01'
  AND  zpu."Zone" = 'East Harlem North'
GROUP BY dropoff_zone
ORDER BY MAX(tip_amount) DESC
LIMIT 1;
```
