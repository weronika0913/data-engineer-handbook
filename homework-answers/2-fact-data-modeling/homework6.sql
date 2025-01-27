-- Insert new data into the host_cumulated table
INSERT INTO host_cumulated 
WITH yesterday AS (
    -- Get data from the last day of the previous year
    SELECT * 
    FROM host_cumulated 
    WHERE date = DATE('2022-12-31')
),

today AS (
    -- Get today's host activity data
    SELECT 
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events 
    WHERE DATE(event_time) = DATE('2023-01-01') -- Filter for today's events
    GROUP BY host, DATE(CAST(event_time AS TIMESTAMP)) -- Group by host and date
)

-- Combine yesterday's and today's data into the final result
SELECT 
    COALESCE(y.host, t.host) AS host, -- Use the host from either table, preferring non-NULL values
    CASE 
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active] -- If yesterday's list is empty, start with today's date
        WHEN t.date_active IS NULL THEN y.host_activity_datelist -- If no activity today, keep yesterday's list
        ELSE ARRAY[t.date_active] || y.host_activity_datelist -- Otherwise, combine today's date with yesterday's list
    END AS host_activity_datelist,
    DATE(COALESCE(t.date_active, y.date + INTERVAL '1 day')) AS date -- Use today's date or increment yesterday's date
FROM yesterday AS y
FULL OUTER JOIN today AS t ON y.host = t.host; -- Match hosts from both datasets
