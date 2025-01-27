-- Create a CTE (`users`) to retrieve data from the `user_devices_cumulated` table for the last day of the month
WITH users AS (
    SELECT * 
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-31') -- Filter for data from the last day of January 2023
),

-- Generate a series of dates from January 1st to January 31st, 2023
series AS (
    SELECT * 
    FROM generate_series(
        DATE('2023-01-01'), -- Start date of the series
        DATE('2023-01-31'), -- End date of the series
        INTERVAL '1 day' -- Increment by 1 day
    ) AS series_date -- Alias for each generated date
),

-- Create a placeholder for integer calculations based on date activity
placeholder_ints AS (
    SELECT 
        -- Check if the series date exists in the `dates_activate` array for the user
        CASE 
            WHEN dates_activate @> ARRAY[DATE(series_date)] -- If `series_date` is part of the `dates_activate` array
            THEN CAST(POW(2, 32 - (DATE - DATE(series_date))) AS BIGINT) 
            -- Assign an integer value using a bit-shifting approach, where each date corresponds to a specific bit
            ELSE 0 -- If the date is not in `dates_activate`, assign 0
        END AS placeholder_int_value, -- Calculated placeholder integer value
        * -- Include all other columns from `users` and `series`
    FROM users 
    CROSS JOIN series -- Combine every user with every date in the series
    -- This ensures we calculate a value for all possible dates in January for each user
)

-- Aggregate the results to calculate a 32-bit integer representing activity for each user and browser type
SELECT 
    user_id, -- Unique identifier for the user
    browser_type, -- The browser type used by the user
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int 
    -- Sum the placeholder integer values to create a compressed representation of date activity
    -- Convert the result into a 32-bit binary format (`bit(32)`)
FROM placeholder_ints
GROUP BY user_id, browser_type 
-- Group by user_id and browser_type to calculate the datelist_int for each combination
