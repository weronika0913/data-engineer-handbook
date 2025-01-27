insert into user_devices_cumulated 
with yesterday as (
   -- Retrieve data from the `user_devices_cumulated` table for the previous day
    select *
    from user_devices_cumulated
    where date = date('2023-01-11')
),

    today as (
        select
        --get the list of users, browser_type where they are not null for a current date
         cast(e.user_id as text) as user_id,
         d.browser_type, 
		 date(CAST(event_time as timestamp)) as date_active
    from events as e
    join devices  as d on e.device_id = d.device_id
    where date(CAST(event_time as timestamp)) = date('2023-01-12') 
    and user_id is not null
    GROUP BY user_id, date(CAST(event_time as timestamp)), browser_type

)
-- Combine `today` and `yesterday` datasets into a final result
select 
	distinct -- Ensure no duplicate rows in the final result
    COALESCE(t.user_id,y.user_id) as user_id, 
    COALESCE(t.browser_type,y.browser_type) as browser_type,
        -- Handle merging date arrays for user activity:
	case 
        when y.dates_activate is null
        then array[t.date_active]
        when t.date_active is null then y.dates_activate
        else y.dates_activate || array[t.date_active] 
        end as dates_activate,
    DATE(COALESCE(t.date_active, y.date + interval '1 day')) as date

from today as t
    full outer join yesterday as y on y.user_id = t.user_id
    -- Perform a FULL OUTER JOIN to ensure all users from both `today` and `yesterday` are included
    -- This is critical to capture both new users and updates to existing users' activity.