-- Show weather table --
SELECT *
FROM weather;

-- Temperature Trends: Highest and Lowest Temperatures per City
SELECT City, MAX(Temperature) AS Max_Temperature, Min(Temperature) AS Min_Temperature
FROM weather
GROUP BY City;

-- Rolling 7-Day Average Temperature per City --
WITH RollingAvg AS (
	SELECT City, Date(Timestamp) AS Date, 
    Avg(Temperature) OVER (PARTITION BY City ORDER BY Timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Avg_Temp_7day
FROM weather
)
SELECT * FROM RollingAvg;

-- Extreme Weather Alerts --
SELECT City, Temperature, Wind_Speed_mph,
		CASE
			WHEN Temperature < 32 THEN 'Freeze Warning'
			WHEN Wind_Speed_mph BETWEEN 0 AND 20 THEN 'Breezy'
            WHEN Wind_Speed_mph BETWEEN 21 AND 25 THEN 'Windy'
            WHEN Wind_Speed_mph BETWEEN 26 AND 39 THEN 'Very Windy'
            WHEN Wind_Speed_mph BETWEEN 40 AND 57 THEN 'High Wind'
            WHEN Wind_Speed_mph > 58 THEN 'DAMAGING HIGH WIND'
			ELSE 'Normal'
		END AS Weather_Alert
FROM weather
WHERE Temperature < 32 OR Wind_Speed_mph > 39
;

-- Daily Weather Summaries --
SELECT City, DATE(Timestamp) AS DATE, 
       AVG(Temperature) AS Avg_Temperature,
		AVG(Feels_Like_F) AS Avg_Feels_Like,
		MIN(Temperature) AS Min_Temperature,
       MAX(Temperature) AS Max_Temperature,    
       AVG(Humidity_) AS Avg_Humidity, 
       AVG(Wind_Speed_mph) AS Avg_Wind_Speed
FROM weather
GROUP BY City, DATE(Timestamp);

-- Comparing Cities: Rank by Temperature and Humidity
SELECT City, Temperature, Humidity_,
       RANK() OVER (ORDER BY Temperature DESC) AS Temp_Rank,
       RANK() OVER (ORDER BY Humidity_ DESC) AS Humidity_Rank
FROM weather;

-- Most Common Weather Condition per City
SELECT City, Weather_Condition, COUNT(*) AS Condition_Count
FROM weather
GROUP BY City, Weather_Condition
ORDER BY City, Condition_Count DESC;

-- Just for Fun --
-- Advanced Weather Analysis Query: Multi-Level Aggregation & Calculations
WITH WeatherStats AS (
    SELECT City, 
           DATE(Timestamp) AS Date, 
           AVG(Temperature) AS Avg_Temperature, 
           MAX(Temperature) AS Max_Temperature, 
           MIN(Temperature) AS Min_Temperature, 
           AVG(Humidity_) AS Avg_Humidity, 
           MAX(Wind_Speed_mph) AS Max_Wind_Speed, 
           COUNT(*) AS Readings_Count
    FROM weather
    GROUP BY City, DATE(Timestamp)
),
WeatherRankings AS (
    SELECT City, 
           Avg_Temperature,
           RANK() OVER (ORDER BY Avg_Temperature DESC) AS Temp_Rank,
           Avg_Humidity,
           RANK() OVER (ORDER BY Avg_Humidity DESC) AS Humidity_Rank,
           Max_Wind_Speed,
           RANK() OVER (ORDER BY Max_Wind_Speed DESC) AS Wind_Rank
    FROM WeatherStats
)
SELECT ws.City, 
       ws.Date,
       ws.Avg_Temperature, 
       ws.Max_Temperature, 
       ws.Min_Temperature, 
       ws.Avg_Humidity, 
       ws.Max_Wind_Speed,
       ws.Readings_Count,
       wr.Temp_Rank, 
       wr.Humidity_Rank, 
       wr.Wind_Rank,
       CASE 
           WHEN ws.Max_Temperature > 100 THEN 'Extreme Heat'
           WHEN ws.Min_Temperature < 32 THEN 'Freezing'
           WHEN ws.Max_Wind_Speed > 40 THEN 'Stormy'
           ELSE 'Normal'
       END AS Weather_Status
FROM WeatherStats ws
JOIN WeatherRankings wr ON ws.City = wr.City
ORDER BY ws.Date DESC, ws.City;
