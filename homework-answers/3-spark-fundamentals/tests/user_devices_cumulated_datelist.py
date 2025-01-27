from pyspark.sql import SparkSession





def do_user_devices_cumulated_datelist(spark, events_df, devices_df, user_devices_cumulated_df, ds):
    query = f"""
with yesterday as (
   -- Retrieve data from the `user_devices_cumulated` table for the previous day
    select *
    from user_devices_cumulated
    where date = '{ds}'
),

    today as (
        select
        --get the list of users, browser_type where they are not null for a current date
         cast(e.user_id as string) as user_id,
         d.browser_type, 
		 date(cast(event_time as timestamp)) as date_active
    from events as e
    join devices  as d on e.device_id = d.device_id
    where date(cast(event_time as timestamp)) = date('2023-01-02') 
    and user_id is not null
    GROUP BY user_id, date(cast(event_time as timestamp)), browser_type

)
-- Combine `today` and `yesterday` datasets into a final result
select 
	distinct -- Ensure no duplicate rows in the final result
    COALESCE(t.user_id,y.user_id) as user_id, 
    COALESCE(t.browser_type,y.browser_type) as browser_type,
        -- Handle merging date arrays for user activity:
	case 
        when y.dates_activate is null
        then array(t.date_active)
        when t.date_active is null then y.dates_activate
        else y.dates_activate || array(t.date_active)
        end as dates_activate,
    DATE(COALESCE(t.date_active, y.date + interval '1 day')) as date

from today as t
    full outer join yesterday as y on y.user_id = t.user_id
    """
    events_df.createOrReplaceTempView("events")
    devices_df.createOrReplaceTempView("devices")
    user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")
    return spark.sql(query)


def main():
    ds = '2023-01-01'
    spark = SparkSession.builder \
      .master("local") \
      .appName("user_devices_cumulated ") \
      .getOrCreate()
    output_df = do_user_devices_cumulated_datelist(spark, spark.table("user_devices_cumulated"), spark.table("events"), spark.table("devices"), ds)
    output_df.write.mode("overwrite").insertInto("user_devices_cumulated")