# 🔥 London Fire Brigade Incident Call Analysis (Jan–Apr 2017)

This project explores emergency incident call data from the **London Fire Brigade**, using a public dataset hosted on **Google BigQuery**.

## 🛠 Tools Used
- **SQL** (BigQuery)
- **Tableau Public** for visualisation

## 📁 Files Included
- `London_Fire_Brigade_Dashboard.twbx` – Tableau Packaged Workbook
- `query.sql` – SQL query used to extract and analyze the data 

## 📊 Key Insights
- Emergency call trends over time
- response patterns
- Outlier detection using ±3 standard deviations
- Station-level deployment metrics

## 🔗 Dashboard Link
[View on Tableau Public]
https://public.tableau.com/app/profile/emma.schenegg/viz/LondonFireBrigadeIncidentCallAnalysisJanApr2017/Incidentscallsbytypeandborough 


## 📌 About
This project is part of my data portfolio and demonstrates my ability to:
- Extract real-world data using SQL
- Perform statistical analysis
- Build interactive dashboards with Tableau

# SQL Analysis: London Fire Brigade Service Calls (BigQuery)

This project analyses the **London Fire Brigade Service Calls** dataset for 2017, focusing on data cleaning, incident volumes, and identifying unusual activity patterns.

Dataset: `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`

---

## 1. Data Cleaning

### 1.1 Checking Column Data Types
We first examine the schema to ensure correct and consistent data types.

```sql
SELECT
  column_name,
  data_type
FROM
  `bigquery-public-data.london_fire_brigade.INFORMATION_SCHEMA.COLUMNS`;

✅ All columns use the correct formats.

### 1.2 Checking for Null Values Using Dynamic SQL
Instead of manually counting NULLs per column, dynamic SQL generates COUNTIF statements for all 34 fields.

sql
Copy code
DECLARE sql_query STRING;

SET sql_query = (
  SELECT STRING_AGG(
    FORMAT("COUNTIF(%s IS NULL) AS %s_nulls", column_name, column_name)
  )
  FROM `bigquery-public-data.london_fire_brigade.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'fire_brigade_service_calls'
);

EXECUTE IMMEDIATE FORMAT("""
SELECT
  COUNT(*) AS total_rows,
  %s
FROM `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
""", sql_query);
Findings:

special_service_type: 22,166 nulls

easting_m, northing_m, postcode_full: 15,411 nulls

first_pump_arriving_* fields: 1,819 nulls

second_pump_arriving_* fields: 20,281 nulls

Key temporal and geographic identifiers: 0 nulls ✅

### 1.3 Checking for Duplicate Records
sql
Copy code
SELECT
  incident_number,
  COUNT(*) AS count
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
  incident_number
HAVING
  COUNT(*) > 1;

✅ No duplicates found — incident_number is unique.

### 1.4 Validating Numeric Columns
sql
Copy code
SELECT 
  COUNTIF(first_pump_arriving_attendance_time < 0) AS invalid_first_pump_time,
  COUNTIF(second_pump_arriving_attendance_time < 0) AS invalid_second_pump_time,
  COUNTIF(num_stations_with_pumps_attending < 0) AS invalid_station_count,
  COUNTIF(num_pumps_attending < 0) AS invalid_pump_count
FROM `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`;
✅ No negative values detected.

### 1.5 Check the time span covered by the dataset.

SELECT
  MIN(DATE(date_of_call)) AS start_date,
  MAX(DATE(date_of_call)) AS end_date
FROM
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`;

-- from 01/01/2017 to 30/04/2017

## 2. Incident Volume and Frequency (2017

### 2.1 Total Incidents by Type

sql
Copy code
SELECT
  incident_group,
  COUNT(*) AS group_category
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
  incident_group
ORDER BY
  group_category;

### 2.2 Incidents per Month
sql
Copy code
SELECT
  DATE_TRUNC(date_of_call, MONTH) AS month,
  incident_group,
  COUNT(*) AS total
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY month, incident_group
ORDER BY month, total DESC;

### 2.3 Incidents by Weekday
sql
Copy code
SELECT
  FORMAT_DATE('%A', date_of_call) AS day_of_week,
  COUNT(*) AS total_incidents
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY day_of_week
ORDER BY total_incidents DESC;

### 2.4 Busiest Times of Day
sql
Copy code
SELECT
  hour_of_call,
  COUNT(*) AS incident_count
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
  hour_of_call
ORDER BY 
  incident_count DESC;
Observations:

Peak: 18:00 (2,187 calls)

Low: 00:00 (485 calls)


## 3. Outlier Detection
### 3.1 Unusually Busy Hours
sql
Copy code
WITH hourly_count AS (
  SELECT
    EXTRACT(HOUR FROM time_of_call) AS hour_of_day,
    COUNT(*) AS total_incidents
  FROM
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
  GROUP BY hour_of_day
),
stats AS (
  SELECT
    AVG(total_incidents) AS avg_incidents,
    STDDEV(total_incidents) AS std_dev
  FROM hourly_count
)
SELECT
  hour_of_day,
  total_incidents
FROM
  hourly_count, stats
WHERE
  total_incidents > avg_incidents + 1.5 * std_dev
ORDER BY total_incidents DESC;

📌 18:00 is a moderate outlier (+1.5 SD from mean).

### 3.2 Unusually Busy Days
sql
Copy code
WITH daily_count AS (
  SELECT 
    DATE(date_of_call) AS call_date,
    COUNT(*) AS total_incidents
  FROM 
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
  GROUP BY call_date
),
stats AS (
  SELECT
    AVG(total_incidents) AS avg_incidents,
    STDDEV(total_incidents) AS std_dev
  FROM daily_count
)
SELECT
  call_date,
  total_incidents
FROM
  daily_count, stats
WHERE
  total_incidents > avg_incidents + 3 * std_dev
ORDER BY total_incidents DESC;

📌 Feb 23, 2017 had 525 incidents due to Storm Doris (extreme weather event).

## Geographical Distribution

### 4.1 Incidents by Boroughs

SELECT
      borough_name,COUNT(*) AS total_incidents
FROM
      `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
    borough_name
ORDER BY
    total_incidents DESC

-- WESTMINSTER  (2469) > CITY OF LONDON (367)

 
### 4.2 Incident types by borough

SELECT
  incident_group, 
  borough_name,
  COUNT(*) AS incidents
FROM
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
  incident_group,
  borough_name
ORDER BY 
  borough_name

### 4.3 Incident calls volume by area by months

SELECT
      borough_name,
      FORMAT_DATE('%B', DATE_TRUNC(date_of_call, MONTH)) AS Month,
      COUNT(*) AS Total_Incidents
FROM
      `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
     borough_name, Month 
ORDER BY
      borough_name


## First Pump

### 5.1 Average Response Time by Borough

SELECT
  borough_name,
  ROUND(AVG(first_pump_arriving_attendance_time),2) AS avg_first_pump_time
FROM
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE
    first_pump_arriving_attendance_time IS NOT NULL
GROUP BY
    borough_name
ORDER BY
    avg_first_pump_time 

quicker to arrive: Kensingston and Chelsea (265s or 4.4min), slower: (373s or 6.2min)


### 5.2 How many calls by borough resulted in help arriving 15min or more at location 

SELECT
borough_name,
COUNT(*) AS total_pump_15min
FROM
`bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE 
first_pump_arriving_attendance_time> 900
GROUP BY
borough_name
ORDER BY
 total_pump_15min DESC;


### 5.3 Number of first pump sent by station

SELECT
      first_pump_arriving_deployed_from_station,
      COUNT(*) as number_first_pump_sent
FROM
      `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE
      first_pump_arriving_deployed_from_station IS NOT NULL
GROUP BY
      first_pump_arriving_deployed_from_station
ORDER BY
      number_first_pump_sent DESC


### 5.4 How long in average do each station take to deploy first pump

SELECT
first_pump_arriving_deployed_from_station,
COUNT(*) AS number_pump_sent_from_station,
ROUND(AVG(first_pump_arriving_attendance_time),2) AS avg_attendance_time
FROM
`bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE
first_pump_arriving_deployed_from_station IS NOT NULL
AND first_pump_arriving_attendance_time IS NOT NULL
GROUP BY 
first_pump_arriving_deployed_from_station
ORDER BY
avg_attendance_time


--Quicker Battersea, Slower Staines then Ruislip (but only one intervention in Staines)


### 5.5 Identifying Outliers in First Pump Response Times

Multiplying the standard deviation by 3 (±3σ) helps identify outliers by flagging values that fall outside the typical 99.7% range of a normally distributed dataset.
WITH response_stats AS (
SELECT 
AVG(first_pump_arriving_attendance_time) AS avg_time,
STDDEV(first_pump_arriving_attendance_time) AS std_dev
FROM 
`bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE 
first_pump_arriving_attendance_time IS NOT NULL
)

SELECT 
date_of_call,
first_pump_arriving_deployed_from_station,
first_pump_arriving_attendance_time
FROM 
`bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`, response_stats
WHERE 
first_pump_arriving_attendance_time > avg_time + 3 * std_dev
OR first_pump_arriving_attendance_time < avg_time - 3 * std_dev
ORDER BY 
first_pump_arriving_attendance_time DESC;

 
## Second pump

### 6.1 Which stations most often respond as the second pump?

SELECT 
second_pump_arriving_deployed_from_station AS station,
COUNT(*)AS number_of_times_responded
FROM 
`bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE 
second_pump_arriving_deployed_from_station IS NOT NULL
GROUP BY 
station
ORDER BY 
number_of_times_responded DESC

 
### 6.2 Which station send more help to other station for the second pump

SELECT 
  second_pump_arriving_deployed_from_station AS helper_station,
  COUNT(*) AS times_helped_other_station
FROM
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE
  second_pump_arriving_deployed_from_station IS NOT NULL
  AND first_pump_arriving_deployed_from_station IS NOT NULL
  AND second_pump_arriving_deployed_from_station != first_pump_arriving_deployed_from_station
GROUP BY
  helper_station
ORDER BY
  times_helped_other_station DESC

-- Biggest help: Fulham, Hammersmith, Chealsea

### 6.3 Second pump arriving quicker than first pump on incident

SELECT
      second_pump_arriving_deployed_from_station,
      COUNT(*) AS total_second_pump_quicker_than_first_pump
FROM
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE
      second_pump_arriving_attendance_time<first_pump_arriving_attendance_time
GROUP BY
      second_pump_arriving_deployed_from_station
ORDER BY
      total_second_pump_quicker_than_first_pump DESC


### 6.4 Count of second pump dispatched from same station as first pump

SELECT
      second_pump_arriving_deployed_from_station,
      COUNT(*) AS total_second_pump_same_as_first_pump
FROM
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE
      second_pump_arriving_deployed_from_station=first_pump_arriving_deployed_from_station
GROUP BY
      second_pump_arriving_deployed_from_station
ORDER BY
      total_second_pump_same_as_first_pump DESC


## Incident Types and Property Insights

### 7.1 Incidents by Type by property category

SELECT
  incident_group, COUNT(*) AS total_incidents
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
  incident_group
ORDER BY
  total_incidents DESC

-- False alarms (15732), Special services (10081), Fires (6434) 

 
### 7.2  Incidents by property types 

SELECT
  property_type, COUNT(*) AS total_incidents
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
  property_type
ORDER BY
  total_incidents DESC
LIMIT 10

 
  ---Flats/Maisonettes 4 to 9 storeys (3823)> House-single occupancy (3784)

### 7.3 Top property type involved in Incidents by borough
SELECT
  property_type, 
  borough_name,
  COUNT(*) AS incidents
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
WHERE
      borough_name != "NOT GEO-CODED"
GROUP BY
      borough_name, property_type
ORDER BY
      Incidents DESC 


## Holidays/ Weekdays/ Weekends

### 8.1 Are there spikes in incidents tied to seasons or holidays?

CREATE TABLE calm-nation-467010-r3.london_data.daily_count AS (
SELECT
  DATE(date_of_call) AS Date,
  CASE
    WHEN DATE (date_of_call)IN 
  ('2017-01-01', '2017-01-02', '2017-04-14', '2017-04-17')  --New year/ Easter Monday/ Substitute bank holidays (because 1Jan was a Sunday)/Good Friday
    OR DATE(date_of_call) BETWEEN '2017-02-13'AND '2017-02-17' --Half-Term School Break
    OR DATE (date_of_call) BETWEEN '2017-04-03' AND '2017-04-17' --Easter School Holidays
  THEN 'Holidays'
  ELSE 'Non-Holidays'
 END AS day_type,
 incident_number,
 borough_name,
 COUNT (*) AS total_incidents
FROM
    `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
GROUP BY
  date,day_type, incident_number, borough_name)

SELECT
  day_type,
  COUNT(*) AS number_of_days,
  SUM(total_incidents) AS total_incidents,
  AVG(total_incidents) AS avg_daily_incidents,
  ROUND(100* AVG(total_incidents)/ SUM(AVG(total_incidents)) OVER(), 2) AS pct_of_avg_daily_incidents
FROM
  daily_counts
GROUP BY
  day_type
ORDER BY
  day_type

-- Holidays (51.7%), Non-Holidays(48.3%)


### 8.2 Average Daily incident calls compared to Overall Average Holidays vs Non-Holidays(in %)

-- AVG per day_type
WITH
avg_by_type AS(
  SELECT 
    day_type,
    AVG(total_incidents) AS avg_daily_incidents
  FROM calm-nation-467010-r3.london_data.daily_count
  GROUP BY day_type), 

-- Overall average

overall_average AS(
  SELECT
  AVG(total_incidents) AS avg_all_days
  FROM calm-nation-467010-r3.london_data.daily_count
)

-- Calculate percentage comparison

SELECT
  a.day_type,
  a.avg_daily_incidents,
  o.avg_all_days,
  ROUND(a.avg_daily_incidents/o.avg_all_days*100, 2) AS pct_of_overall
  FROM avg_by_type a
  CROSS JOIN overall_average o


 

### 8.3 Holidays vs Non-Holidays breakdown

SELECT
  day_type,
  date_of_call,
  COUNT (*) AS total_incidents
FROM
  calm-nation-467010-r3.london_data.daily_count AS dc
INNER JOIN 
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls` AS f
ON 
    dc.date = f.date_of_call
GROUP BY
  date_of_call, day_type
ORDER BY
  date_of_call

 
### 8.4 Incident type by day type

CREATE TABLE `calm-nation-467010-r3.london_data.day_type` AS
SELECT
  incident_group,
  DATE(date_of_call) AS date
  CASE
    WHEN DATE(date_of_call) IN ('2017-01-01', '2017-01-02','2017-04-14', '2017-04-17')
      OR DATE(date_of_call) BETWEEN '2017-02-13' AND '2017-02-17'
      OR DATE(date_of_call) BETWEEN '2017-04-03' AND '2017-04-17'
      THEN 'Holiday'
    WHEN EXTRACT(DAYOFWEEK FROM DATE(date_of_call)) IN (1,7)
      THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type
FROM
  `bigquery-public-data.london_fire_brigade.fire_brigade_service_calls`
)

  SELECT
    day_type,
    incident_group,
    COUNT(*) AS total_incidents
  FROM daily_count
  GROUP BY day_type, incident_group
  ORDER BY day_type, total_incidents DESC

 

### 8.5 Incident calls during weekday by borough

SELECT
  borough_name, 
  day_type,
  COUNT(*) AS total_incidents_by_borough
FROM
  calm-nation-467010-r3.london_data.day_type
WHERE
  day_type = 'Weekday'
GROUP BY
  borough_name, day_type
ORDER BY
  total_incidents_by_borough DESC

 

### 8.6 Incident calls during weekends by borough

SELECT
  borough_name, 
  day_type,
  COUNT(*) AS total_incidents_by_borough
FROM
  calm-nation-467010-r3.london_data.day_type
WHERE
  day_type = 'Weekend'
GROUP BY
  borough_name, day_type
ORDER BY
  total_incidents_by_borough DESC

 

### 8.7 Incident calls during holidays by borough

SELECT
  borough_name, 
  day_type,
  COUNT(*) AS total_incidents_by_borough
FROM
  calm-nation-467010-r3.london_data.day_type
WHERE
  day_type = 'Holiday'
GROUP BY
  borough_name, day_type
ORDER BY
  total_incidents_by_borough DESC
	
