from chispa.dataframe_comparer import *
from datetime import datetime

from ..jobs.user_devices_cumulated_datelist import do_user_devices_cumulated_datelist
from collections import namedtuple

Events = namedtuple("events", "user_id event_time device_id")
Devices = namedtuple("devices", "device_id browser_type")
UserDevicesCumulated = namedtuple("user_devices_cumulated", "user_id browser_type dates_activate date")




def test_when_no_previous_user_cumulated_data(spark):

    event_data = [
        Events(user_id="1", event_time="2023-01-02", device_id=1),
        Events(user_id="2", event_time="2023-01-02", device_id=2),
        Events(user_id="3", event_time="2023-01-02", device_id=3)
    ]

    device_data = [
        Devices(device_id=1, browser_type="chrome"),
        Devices(device_id=2, browser_type="firefox"),
        Devices(device_id=3, browser_type="safari")
    ]

    events_df = spark.createDataFrame(event_data)
    devices_df = spark.createDataFrame(device_data)
    user_devices_cumulated_df= spark.createDataFrame([],"user_id STRING, browser_type STRING, dates_activate ARRAY<date>, date DATE")

    actual_df = do_user_devices_cumulated_datelist(spark, events_df, devices_df, user_devices_cumulated_df, "2023-01-01")

    current_date = datetime.strptime("2023-01-02", "%Y-%m-%d").date()

    expected_values = [
        UserDevicesCumulated(user_id="1", browser_type="chrome", dates_activate=[current_date],date=current_date),
        UserDevicesCumulated(user_id="2", browser_type="firefox", dates_activate=[current_date],date=current_date),
        UserDevicesCumulated(user_id="3", browser_type="safari", dates_activate=[current_date],date=current_date)
    ]


    expected_df = spark.createDataFrame(expected_values)

    actual_df = actual_df.sort("user_id")
    expected_df = expected_df.sort("user_id")
    assert_df_equality(actual_df,expected_df)
    