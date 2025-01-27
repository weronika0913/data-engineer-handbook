-- Insert or update records in the host_activity_reduced table
INSERT INTO host_activity_reduced 
WITH yesterday AS (
    -- Fetch data for the first day of the month (existing records)
    SELECT * 
    FROM host_activity_reduced
    WHERE month_start = DATE('2023-01-01')
),

today AS (
    -- Get today's activity data
    SELECT 
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS date, -- Today's activity date
        COUNT(DISTINCT(user_id)) AS unique_visitor, -- Count of unique visitors
        COUNT(1) AS hit_array -- Count of all events
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-02') -- Filter for today's events
    GROUP BY host, DATE(CAST(event_time AS TIMESTAMP)) -- Group data by host and date
)

-- Combine data from yesterday and today
SELECT 
    COALESCE(y.host, t.host) AS host, -- Use host from either yesterday or today
    COALESCE(y.month_start, DATE_TRUNC('month', t.date)) AS month_start, -- Start of the current month
    CASE 
        WHEN y.hit_array IS NOT NULL 
            THEN y.hit_array || ARRAY[COALESCE(t.hit_array, 0)] -- Append today's hits to yesterday's
        WHEN y.hit_array IS NULL 
            THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(t.hit_array, 0)] -- Initialize if no hits from yesterday
    END AS hit_array,
    CASE 
        WHEN y.unique_visitor IS NOT NULL 
            THEN y.unique_visitor || ARRAY[COALESCE(t.unique_visitor, 0)] -- Append today's unique visitors
        WHEN y.unique_visitor IS NULL 
            THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(t.unique_visitor, 0)] -- Initialize if no unique visitors from yesterday
    END AS unique_visitor
FROM yesterday AS y
FULL OUTER JOIN today AS t ON y.host = t.host -- Combine data from both datasets
ON CONFLICT (host, month_start) -- Handle conflicts on host and month_start keys
DO UPDATE 
SET 
    unique_visitor = EXCLUDED.unique_visitor, -- Update unique visitors
    hit_array = EXCLUDED.hit_array; -- Update hit array
