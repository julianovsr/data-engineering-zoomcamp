# Module 1 - Homework
## Question 1:
In terminal do:
`docker run -it --entrypoint=bash python:3.13`

Then
`pip -V`

Result:
Version 25.3


## Question 2:
Answer: postgres:5432

## Question 3:
SQL:
`SELECT *
FROM green_trip_data dt
WHERE dt.lpep_pickup_datetime between '2025-11-01' and '2025-12-01'
AND dt.trip_distance <= 1.0`

Answer: 8007

## Question 4:
SQL:
`SELECT lpep_pickup_datetime, trip_distance
FROM green_trip_data dt
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1`

Answer: 2025-11-14

## Question 5:
SQL:
`SELECT tz."Zone", count(*)
FROM green_trip_data dt
JOIN taxi_zones tz ON tz."LocationID" = dt."PULocationID"
GROUP BY tz."Zone"
ORDER BY count(*) DESC 
LIMIT 1`

Answer: East Harlem North - 12049 Trips

## Question 6:
SQL:
`SELECT tz_DO."Zone", dt.tip_amount
FROM green_trip_data dt
JOIN taxi_zones tz_PU ON tz_PU."LocationID" = dt."PULocationID"
JOIN taxi_zones tz_DO ON tz_DO."LocationID" = dt."DOLocationID"
WHERE tz_PU."Zone" = 'East Harlem North'
ORDER BY dt.tip_amount DESC
LIMIT 1`

Answer: Yorkville West - $81.89

## Question 7:

Answer: terraform init, terraform apply -auto-approve, terraform destroy
