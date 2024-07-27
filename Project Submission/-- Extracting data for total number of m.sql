-- Extracting data for total number of metrocar app downloads --
SELECT COUNT (DISTINCT app_download_key)
FROM app_downloads;

-- Extracting data for number of user signups
SELECT COUNT (DISTINCT user_id)
FROM signups;

-- Extracting data for number of requested and completed ride requests through the app
WITH user_ride_status AS (
    SELECT
        user_id,
        MAX(
            CASE
                WHEN dropoff_ts IS NOT NULL
                THEN 1
                ELSE 0
            END
        ) AS ride_completed
    FROM ride_requests
    GROUP BY user_id
)
SELECT
    COUNT(*) AS Total_users_ride_requested,
    SUM(ride_completed) AS Total_users_ride_completed
FROM user_ride_status;

-- Retrieving data for number of rides requested and unique users ride requests--
SELECT COUNT(DISTINCT user_id) AS unique_users_ride_request
FROM ride_requests
WHERE request_ts IS NOT NULL;


--Retrieving average time of a ride from pickup to dropoff--
SELECT ROUND(AVG(EXTRACT(EPOCH FROM dropoff_ts - pickup_ts)),6) AS average_time_of_ride
FROM ride_requests;

-- Retrieving data for number of rides accepted per driver --
SELECT driver_id, COUNT(accept_ts) AS accepted_ride_count
FROM ride_requests
WHERE accept_ts IS NOT NULL 
GROUP BY driver_id;


--Retrieving data for number of rides per successful payments -- 
SELECT COUNT(ride_id) AS number_of_successful_payments,SUM(purchase_amount_usd) AS total_amount_of_successful_purchase
FROM transactions
WHERE charge_status = 'Approved';

--Retrieving data for ride requests for each platform --
SELECT platform , COUNT(request_ts) AS number_ride_requests
FROM metrocar
GROUP BY platform;


-- Data for number of rides requested and completed through the app --(4)
SELECT COUNT(request_ts) AS rides_completed_through_app,COUNT(EXTRACT(EPOCH FROM dropoff_ts)) AS rides_completed_through_app
FROM ride_requests
WHERE request_ts IS NOT NULL;


-- Data for number of rides accepted by a driver--
SELECT COUNT(accept_ts) AS accepted_ride_count
FROM ride_requests
WHERE accept_ts IS NOT NULL 


-- Constructing the funnel for analysis--
--Data extraction for conversation rate at each step of the funnel
WITH user_ride_status AS (
    SELECT
        user_id
    FROM ride_requests
    GROUP BY user_id
),
total_app_download AS (
    SELECT COUNT (DISTINCT app_download_key) AS number_of_downloads
    FROM app_downloads
),
totals AS (
    SELECT
        COUNT(*) AS total_users_signed_up,
        COUNT(DISTINCT urs.user_id) AS total_users_ride_requested
    FROM signups s
    LEFT JOIN user_ride_status urs ON
        s.user_id = urs.user_id
),
completed_rides AS (
    SELECT
        user_id,
        MAX(DISTINCT ride_id) AS ride_completed
    FROM ride_requests
    WHERE dropoff_ts IS NOT NULL
    GROUP BY user_id
 ),
 total_review_count AS (
    SELECT COUNT (DISTINCT user_id) AS number_of_reviews
    FROM reviews
),
funnel_stages AS (
  SELECT
        1 AS funnel_step,
        'downloads' AS funnel_name,
        number_of_downloads AS value
    FROM total_app_download  
  
  UNION
  
  SELECT
        2 AS funnel_step,
        'user_signups' AS funnel_name,
        total_users_signed_up AS value
    FROM totals

    UNION

    SELECT
        3 AS funnel_step,
        'ride_requested' AS funnel_name,
        total_users_ride_requested AS value
    FROM totals

    UNION

    SELECT
        4 AS funnel_step,
        'ride_completed' AS funnel_name,
        COUNT(DISTINCT user_id) AS value
    FROM completed_rides
  
  UNION

    SELECT
        5 AS funnel_step,
        'reviews' AS funnel_name,
        number_of_reviews AS value
    FROM total_review_count
)
SELECT 
    *,
    value::float * 100 / LAG(value) OVER (ORDER BY funnel_step) AS conversion_rate,
    LAG(value) OVER (ORDER BY funnel_step) - value AS dropoff_points
FROM 
    funnel_stages
ORDER BY 
    funnel_step;


--Retrieving data for pickup location and Request Timestamp (Additional Analysis)--
SELECT pickup_location,COUNT(request_ts),request_ts AS start_time
FROM ride_requests
WHERE dropoff_ts IS NOT NULL
GROUP BY pickup_location,request_ts;
-----------------------------------------------------------------------------------
SELECT 
    SUBSTRING(pickup_location FROM 1 FOR POSITION(' ' IN pickup_location) - 1) AS Latitude,
    SUBSTRING(pickup_location FROM POSITION(' ' IN pickup_location) + 1) AS Longitude,
    COUNT(request_ts) AS number_of_ride_requests
FROM 
    ride_requests
WHERE 
    dropoff_ts IS NOT NULL
GROUP BY ride_requests.pickup_location;    
-----------------------------------------------------------------------------------------    

WITH PickupLocationData AS (
    SELECT 
        pickup_location,
        request_ts,
        COUNT(request_ts) AS number_of_ride_requests,
        SUBSTRING(pickup_location FROM 1 FOR POSITION(' ' IN pickup_location) - 1) AS Latitude,
        SUBSTRING(pickup_location FROM POSITION(' ' IN pickup_location) + 1) AS Longitude,
        EXTRACT(MONTH FROM request_ts) AS RequestMonth
    FROM 
        ride_requests
    WHERE 
        dropoff_ts IS NOT NULL
  GROUP BY ride_requests.pickup_location,ride_requests.request_ts
)
SELECT 
    pickup_location,Latitude,Longitude,request_ts,number_of_ride_requests,RequestMonth
FROM 
    PickupLocationData
    
-------------------------------------------------------------------------------usable
SELECT 
    SUBSTRING(pickup_location FROM 1 FOR POSITION(' ' IN pickup_location) - 1) AS Latitude,
    SUBSTRING(pickup_location FROM POSITION(' ' IN pickup_location) + 1) AS Longitude,
    EXTRACT(MONTH FROM request_ts) AS Month,
    SUM(1) AS total_ride_requests
FROM 
    ride_requests
WHERE 
    dropoff_ts IS NOT NULL
GROUP BY 
    Latitude,
    Longitude,
    Month
ORDER BY 
    Latitude, 
    Longitude,
    Month;
 -------- Month and Year --------- usable 3.0  
    
    SELECT 
    SUBSTRING(pickup_location FROM 1 FOR POSITION(' ' IN pickup_location) - 1) AS Latitude,
    SUBSTRING(pickup_location FROM POSITION(' ' IN pickup_location) + 1) AS Longitude,
    EXTRACT(YEAR FROM request_ts) AS Year,
    CASE 
        WHEN EXTRACT(MONTH FROM request_ts) IN (1) THEN 'January'
        WHEN EXTRACT(MONTH FROM request_ts) IN (2) THEN 'February'
        WHEN EXTRACT(MONTH FROM request_ts) IN (3) THEN 'March'
        WHEN EXTRACT(MONTH FROM request_ts) IN (4) THEN 'April'
        WHEN EXTRACT(MONTH FROM request_ts) IN (5) THEN 'May'
        WHEN EXTRACT(MONTH FROM request_ts) IN (6) THEN 'June'
        WHEN EXTRACT(MONTH FROM request_ts) IN (7) THEN 'July'
        WHEN EXTRACT(MONTH FROM request_ts) IN (8) THEN 'August'
        WHEN EXTRACT(MONTH FROM request_ts) IN (9) THEN 'September'
        WHEN EXTRACT(MONTH FROM request_ts) IN (10) THEN 'October'
        WHEN EXTRACT(MONTH FROM request_ts) IN (11) THEN 'November'
        WHEN EXTRACT(MONTH FROM request_ts) IN (12) THEN 'December'
        ELSE 'Unknown Month'
    END AS Month,
   
    SUM(1) AS total_ride_requests
FROM 
    ride_requests
WHERE 
    dropoff_ts IS NOT NULL
GROUP BY 
    Latitude,
    Longitude,
    Year,
    Month
ORDER BY 
    Latitude,
    Longitude,
    Year,
    Month;
    
    
-- Important version for trial tomorrow--
    
    SELECT 
    EXTRACT(YEAR FROM request_ts) AS Year,
    CASE 
        WHEN EXTRACT(MONTH FROM request_ts) IN (1) THEN 'January'
        WHEN EXTRACT(MONTH FROM request_ts) IN (2) THEN 'February'
        WHEN EXTRACT(MONTH FROM request_ts) IN (3) THEN 'March'
        WHEN EXTRACT(MONTH FROM request_ts) IN (4) THEN 'April'
        WHEN EXTRACT(MONTH FROM request_ts) IN (5) THEN 'May'
        WHEN EXTRACT(MONTH FROM request_ts) IN (6) THEN 'June'
        WHEN EXTRACT(MONTH FROM request_ts) IN (7) THEN 'July'
        WHEN EXTRACT(MONTH FROM request_ts) IN (8) THEN 'August'
        WHEN EXTRACT(MONTH FROM request_ts) IN (9) THEN 'September'
        WHEN EXTRACT(MONTH FROM request_ts) IN (10) THEN 'October'
        WHEN EXTRACT(MONTH FROM request_ts) IN (11) THEN 'November'
        WHEN EXTRACT(MONTH FROM request_ts) IN (12) THEN 'December'
        ELSE 'Unknown Month'
    END AS Month,
    SUBSTRING(pickup_location FROM 1 FOR POSITION(' ' IN pickup_location) - 1) AS Latitude,
    SUBSTRING(pickup_location FROM POSITION(' ' IN pickup_location) + 1) AS Longitude,
    COUNT(request_ts) AS total_ride_requests
FROM 
    ride_requests
WHERE 
    dropoff_ts IS NOT NULL
GROUP BY 
    Year,
    Month,
    Latitude,
    Longitude
ORDER BY 
    Year,
    Month;    
    
    
-- Data for download, user signups , ride requested, completed, payment and reviews based on platform-- 
-- Downloads --
SELECT platform,COUNT(DISTINCT app_download_key) AS downloads 
FROM metrocar
GROUP BY platform;

--User signups
SELECT platform,COUNT (DISTINCT user_id) AS signups
FROM metrocar
GROUP BY platform;
-- Ride Requested, Ride Accepted , Ride Completed , Payment and Reviews --

-- Ride Requested--
SELECT platform,COUNT(DISTINCT user_id), COUNT(DISTINCT ride_id) AS ride_requested
FROM metrocar
WHERE request_ts IS NOT NULL
GROUP BY platform;


-- Ride Accepted--
SELECT platform,COUNT(DISTINCT user_id), COUNT(DISTINCT ride_id) AS ride_accepted
FROM metrocar
WHERE accept_ts IS NOT NULL
GROUP BY platform;


--Ride Completed-- 
SELECT platform,COUNT(DISTINCT user_id), COUNT(DISTINCT ride_id) AS ride_completed
FROM metrocar
WHERE dropoff_ts IS NOT NULL
GROUP BY platform;

--Payments --
SELECT platform,COUNT(DISTINCT user_id), COUNT(DISTINCT ride_id) AS payments
FROM metrocar
WHERE charge_status = 'Approved'
GROUP BY platform;


--Review--
SELECT platform,COUNT(DISTINCT user_id), COUNT(DISTINCT ride_id) AS review
FROM metrocar
WHERE review_id IS NOT NULL
GROUP BY platform;


-- Full table for platform -- 

SELECT
    funnel_step,
    funnel_name,
    platform,
    user_count,
    ride_count
FROM (
    SELECT
        '1' AS funnel_step,
        'Ride Requested' AS funnel_name,
        platform,
        COUNT(DISTINCT CASE WHEN request_ts IS NOT NULL THEN user_id END) AS user_count,
        COUNT(DISTINCT CASE WHEN request_ts IS NOT NULL THEN ride_id END) AS ride_count
    FROM
        metrocar
    GROUP BY
        platform

    UNION ALL

    SELECT
        '2' AS funnel_step,
        'Ride Accepted' AS funnel_name,
        platform,
        COUNT(DISTINCT CASE WHEN accept_ts IS NOT NULL THEN user_id END) AS user_count,
        COUNT(DISTINCT CASE WHEN accept_ts IS NOT NULL THEN ride_id END) AS ride_count
    FROM
        metrocar
    GROUP BY
        platform

    UNION ALL

    SELECT
        '3' AS funnel_step,
        'Ride Completed' AS funnel_name,
        platform,
        COUNT(DISTINCT CASE WHEN dropoff_ts IS NOT NULL THEN user_id END) AS user_count,
        COUNT(DISTINCT CASE WHEN dropoff_ts IS NOT NULL THEN ride_id END) AS ride_count
    FROM
        metrocar
    GROUP BY
        platform

    UNION ALL

    SELECT
        '4' AS funnel_step,
        'Payment' AS funnel_name,
        platform,
        COUNT(DISTINCT CASE WHEN charge_status = 'Approved' THEN user_id END) AS user_count,
        COUNT(DISTINCT CASE WHEN charge_status = 'Approved' THEN ride_id END) AS ride_count
    FROM
        metrocar
    GROUP BY
        platform

    UNION ALL

    SELECT
        '5' AS funnel_step,
        'Review ' AS funnel_name,
        platform,
        COUNT(DISTINCT CASE WHEN review_id IS NOT NULL THEN user_id END) AS user_count,
        COUNT(DISTINCT CASE WHEN review_id IS NOT NULL THEN ride_id END) AS ride_count
    FROM
        metrocar
    GROUP BY
        platform
) AS funnel_data
ORDER BY CAST(funnel_step AS INT) ASC;


-- Count of total number of rides per month : important--  
    SELECT 
    CONCAT(EXTRACT(YEAR FROM request_ts), '-', 
           CASE EXTRACT(MONTH FROM request_ts)
                WHEN 1 THEN 'January'
                WHEN 2 THEN 'February'
                WHEN 3 THEN 'March'
                WHEN 4 THEN 'April'
                WHEN 5 THEN 'May'
                WHEN 6 THEN 'June'
                WHEN 7 THEN 'July'
                WHEN 8 THEN 'August'
                WHEN 9 THEN 'September'
                WHEN 10 THEN 'October'
                WHEN 11 THEN 'November'
                WHEN 12 THEN 'December'
                ELSE 'Unknown Month'
           END) AS YearMonth,
    SUM(1) AS total_ride_requests
FROM 
    ride_requests
WHERE 
    dropoff_ts IS NOT NULL
GROUP BY 
    YearMonth
ORDER BY 
    YearMonth;


-- Time of the day
SELECT EXTRACT(HOUR FROM request_ts) AS time_of_the_day, COUNT(request_ts) AS number_of_ride_requests
FROM ride_requests
WHERE dropoff_ts IS NOT NULL
GROUP BY time_of_the_day    
ORDER BY time_of_the_day ASC;
    
 -- Extract time of the day from data--
SELECT 
			CASE
      		WHEN EXTRACT(HOUR FROM request_ts) IN (6,7,8,9,10,11) THEN 'Morning'
          WHEN EXTRACT(HOUR FROM request_ts) IN (12,13,14,15,16,17) THEN 'Afternoon'
          WHEN EXTRACT(HOUR FROM request_ts) IN (18,19,20,21,22,23) THEN 'Evening'
          WHEN EXTRACT(HOUR FROM request_ts) IN (00,1,2,3,4,5) THEN 'Midnight'
          ELSE 'Unknown Time'
      END AS time_of_day,
      COUNT(*) AS time_record
   FROM ride_requests
   WHERE dropoff_ts IS NOT NULL 
   GROUP BY time_of_day;
   
   
--Extract month from data based on seasons --
SELECT 
    CASE 
        WHEN EXTRACT(MONTH FROM request_ts) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM request_ts) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM request_ts) IN (9, 10, 11) THEN 'Autumn'
        WHEN EXTRACT(MONTH FROM request_ts) IN (12, 1, 2) THEN 'Winter'
        ELSE 'Unknown Month'
    END AS season_of_year,
    COUNT(*) AS number_ride_requests
FROM ride_requests
WHERE dropoff_ts IS NOT NULL
GROUP BY season_of_year;


--Age range at each stage--

WITH Downloads AS (
    SELECT age_range, COUNT(DISTINCT app_download_key) AS downloads
    FROM metrocar
    GROUP BY age_range
),
UserSignups AS (
    SELECT age_range, COUNT(DISTINCT user_id) AS signups
    FROM metrocar
    GROUP BY age_range
),
RideRequested AS (
    SELECT age_range, COUNT(DISTINCT user_id) AS user_count, COUNT(DISTINCT ride_id) AS ride_requested
    FROM metrocar
    WHERE request_ts IS NOT NULL
    GROUP BY age_range
),
RideAccepted AS (
    SELECT age_range, COUNT(DISTINCT user_id) AS user_count, COUNT(DISTINCT ride_id) AS ride_accepted
    FROM metrocar
    WHERE accept_ts IS NOT NULL
    GROUP BY age_range
),
RideCompleted AS (
    SELECT age_range, COUNT(DISTINCT user_id) AS user_count, COUNT(DISTINCT ride_id) AS ride_completed
    FROM metrocar
    WHERE dropoff_ts IS NOT NULL
    GROUP BY age_range
),
Payments AS (
    SELECT age_range, COUNT(DISTINCT user_id) AS user_count, COUNT(DISTINCT ride_id) AS ride_payments
    FROM metrocar
    WHERE charge_status = 'Approved'
    GROUP BY age_range
),
Review AS (
    SELECT age_range, COUNT(DISTINCT user_id) AS user_count, COUNT(DISTINCT ride_id) AS review_count
    FROM metrocar
    WHERE review_id IS NOT NULL
    GROUP BY age_range
)
SELECT 
    D.age_range,COALESCE(D.downloads, 0) AS downloads, COALESCE(U.signups, 0) AS signups, COALESCE(R.user_count, 0) AS ride_requested_users, COALESCE(R.ride_requested, 0) AS ride_requested, COALESCE(A.user_count, 0) AS ride_accepted_users, COALESCE(A.ride_accepted, 0) AS ride_accepted, COALESCE(C.user_count, 0) AS ride_completed_users, COALESCE(C.ride_completed, 0) AS ride_completed, COALESCE(P.user_count, 0) AS ride_payments_users, COALESCE(P.ride_payments, 0) AS ride_payments, COALESCE(RV.user_count, 0) AS review_users, COALESCE(RV.review_count, 0) AS review_count
   
FROM Downloads D
LEFT JOIN UserSignups U ON D.age_range = U.age_range
LEFT JOIN RideRequested R ON D.age_range = R.age_range
LEFT JOIN RideAccepted A ON D.age_range = A.age_range
LEFT JOIN RideCompleted C ON D.age_range = C.age_range
LEFT JOIN Payments P ON D.age_range = P.age_range
LEFT JOIN Review RV ON D.age_range = RV.age_range;


--Driver Accepted--
SELECT COUNT(ride_id)
FROM ride_requests
WHERE accept_ts IS NOT NULL 

--Review rating--
SELECT
    COUNT(review_id) AS total_reviews,
    COUNT(CASE WHEN rating > 2.5 THEN 1 END) AS reviews_with_rating_above_average
FROM
    reviews;
    
    
-- Ride Completed and Payment completed--
SELECT
    COUNT(DISTINCT CASE WHEN dropoff_ts IS NOT NULL THEN ride_id END) AS ride_completed, 
    COUNT(DISTINCT CASE WHEN charge_status = 'Approved' THEN ride_id END) AS payments_completed
FROM
    metrocar;
    
    
 -- Ride Requested , Ride Accepted and Pickup Times -- 
 SELECT ride_id,request_ts,accept_ts,pickup_ts
 FROM ride_requests
 --WHERE pickup_ts IS NULL;
 
 --Average times between ride request and ride accepted--
 SELECT ride_id,ROUND(EXTRACT(EPOCH FROM accept_ts - request_ts)/60) AS time_for_accepted_rides
 FROM ride_requests
 GROUP BY ride_id;
 
 
-- Average times between ride accepted and pickup times --
SELECT ride_id,ROUND(EXTRACT(EPOCH FROM  pickup_ts - accept_ts)/60) AS time_for_pickup
FROM ride_requests

--Count number of drivers to rides requested--
SELECT COUNT(request_ts) AS ride_requested ,COUNT(DISTINCT driver_id) AS number_of_drivers
FROM ride_requests
WHERE accept_ts IS NOT NULL;

--Rides with waiting times between requested and accepted requests with cancelled pickup times--
SELECT ride_id,ROUND(EXTRACT(EPOCH FROM accept_ts - request_ts)/60) AS time_for_accepted_rides,pickup_ts
 FROM ride_requests
 WHERE accept_ts IS NOT NULL AND pickup_ts IS NULL;
 
 
-- Count for ride requested with driver accepted NULL
SELECT
    (SELECT COUNT(request_ts) FROM ride_requests) AS total_ride_request,
    (SELECT COUNT(request_ts) FROM ride_requests WHERE accept_ts IS NULL) AS ride_request_declined;
    
    
-- Rides with longer waiting windows ( greater than ten minutes ) 
SELECT COUNT(ROUND(EXTRACT(EPOCH FROM accept_ts - request_ts)/60)) AS time_for_accepted_rides
FROM ride_requests
WHERE ROUND(EXTRACT(EPOCH FROM accept_ts - request_ts)/60) >= 10 AND pickup_ts IS NULL;

-- Number of ride requests with waiting window less than ten minutes -- 
SELECT COUNT(ROUND(EXTRACT(EPOCH FROM accept_ts - request_ts)/60)) AS time_for_accepted_rides
FROM ride_requests
WHERE ROUND(EXTRACT(EPOCH FROM accept_ts - request_ts)/60) < 10 AND pickup_ts IS NOT NULL;



--Count for number of rides completed --
SELECT COUNT(request_ts) 
FROM ride_requests
WHERE pickup_ts IS NOT NULL;


--Counts for rides declined by driver and rides cancelled due to longer waiting times--
SELECT
    (SELECT COUNT(request_ts) FROM ride_requests) AS total_ride_request,
    (SELECT COUNT(request_ts) FROM ride_requests WHERE accept_ts IS NULL) AS ride_request_declined,
    (SELECT COUNT(*) FROM ride_requests
        WHERE pickup_ts IS NULL
        AND ROUND(EXTRACT(EPOCH FROM accept_ts - request_ts)/60) >= 10
    ) AS longer_waiting_times;
    