CREATE TABLE user_devices_cumulated(
    user_id text,
    browser_type TEXT,
    --the list of dates in the past where user was active
    dates_activate DATE[],
    -- the current date for user
    date DATE,
    PRIMARY KEY(user_id, browser_type,date)
)