
/*
Part A
You have a table populated with trip information (named uber_trip) table with a 
rider_id (uniqueper rider), trip_id (unique per trip), trip_timestamp_utc (the UTC timestamp for when the trip
began), and trip_status, which can either be ‘completed’ or ‘not completed’.

rider_id , trip_id, begintrip_timestamp_utc, trip_status

Write a query to return the trip_id for the 5th completed trip for each rider. If a rider has
completed fewer than five trips, then don’t include them in the results.
*/

SELECT
	rider_id
	, trip_id
FROM 
	(
	SELECT
		rider_id
		, trip_id
		, row_number() OVER (PARTITION BY rider_id ORDER BY begintrip_timestamp_utc) AS n_th_trip
	FROM uber_trip
	WHERE trip_status = 'completed'
	)
WHERE n_th_trip = 5


/*
Part B

You are given three separate tables (named trip_initiated, trip_cancel, and trip_complete) of the
form:
trip_initiated | trip_id, rider_id, driver_id, timestamp
trip_cancel | trip_id, rider_id, driver_id, timestamp
trip_complete | trip_id, rider_id, driver_id, timestamp

Each trip_id in these tables will be unique and only appear once, and a trip will only ever result in
a single cancel event or it will be completed. Write a query to create a single table with one row
per trip event sequence (trip initiated → cancel/complete):

dispatch_events | trip_id, rider_id, driver_id, initiated_ts, cancel_ts,
complete_ts

There should only be a single row per trip with a unique trip_id.
*/

CREATE TABLE dispatch_events AS 
(
	SELECT
		ti.trip_id
		, ti.rider_id 
		, ti.driver_id
		, ti.timestamp AS initiated_ts
		, tx.timestamp AS cancel_ts
		, tc.timestmap AS complete_ts
	FROM trip_initiated ti
	LEFT JOIN trip_cancel tx 
	ON ti.trip_id = tx.trip_id
	LEFT JOIN trip_complete tc
	ON ti.trip_id = tc.trip_id
)


/*
Part C
Write at least one test query to validate the data in the resulting table. Indicate what you would
expect the query to return if the data were valid.
*/

-- test for unique trip_id
SELECT 
	trip_id
FROM dispatch_events
GROUP BY 1
HAVING COUNT(*) > 1

-- I'd expect the result to be empty so that trip_id is unique (it won't be unique if there are duplicate trip_id's in
-- at least one of the source tables)

-- test for event validity
SELECT
	trip_id
FROM dispatch_events
WHERE cancel_ts IS NOT NULL 
AND complete_ts IS NOT NULL

-- I'd expect the result to be empty so that no trip id has both a cancellation and a completion timestamp



