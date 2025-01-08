from chispa.dataframe_comparer import *

from ..jobs.user_activity_cumulation_job import do_user_activity_cumulation
from collections import namedtuple
from pyspark.sql.types import *
from pyspark.sql.functions import col


def test_user_activity_cumulation(spark):
    # Test validates final query output
    
    # Define columns as namedtuple
    Device = namedtuple("Device", "device_id browser_type")
    Event = namedtuple("Event", "user_id device_id event_time")
    UserDevicesCumulated = namedtuple("UserDevicesCumulated","user_id browser_type device_activity_datelist date")
   
    # Historical data
    cumulated_activity = [
    UserDevicesCumulated(7367, "Googlebot", ["2023-01-03", "2023-01-02", "2023-01-01"], "2023-01-04"),
    UserDevicesCumulated(1178, "BLEXBot",["2023-01-03", "2023-01-01"], "2023-01-04"),
    UserDevicesCumulated(4250, "bingbot",["2023-01-04", "2023-01-01"], "2023-01-04"),
    UserDevicesCumulated(9446, "Chrome", ["2023-01-03", "2023-01-01"], "2023-01-04"),
    UserDevicesCumulated(3588, "Chrome Mobile", ["2023-01-03"], "2023-01-04"),
    UserDevicesCumulated(1591, "PetalBot", ["2023-01-02"], "2023-01-04")
    ]

    # Create dataframe for historical data
    src_cumulated = spark.createDataFrame(cumulated_activity)

    # Device data
    devicedata = [
    Device(1234, "Firefox"),
    Device(5678, "bingbot"),
    Device(2345,"Googlebot"),
    Device(5234, "Chrome")
    ]

    # Create dataframe for device data
    src_device = spark.createDataFrame(devicedata) 

    # Daily activity
    eventdata = [
    Event(1409, 1234, "2023-01-05 11:55:28.032000"),
    Event(4250, 5678, "2023-01-05 11:55:28.624000"),
    Event(4250, 5678, "2023-01-05 11:55:28.624000"),
    Event(7367, 2345, "2023-01-05 11:55:28.857000"),
    Event(7367, 2345, "2023-01-05 11:55:28.857000"),
    Event(7367, 2345, "2023-01-05 11:55:28.857000"),
    Event(9446, 5234, "2023-01-05 11:55:28.546000"),
    Event(9446, 5234, "2023-01-05 11:55:28.546000"),
    Event(9446, 5234, "2023-01-05 11:55:28.546000"),
    Event(9446, 5234, "2023-01-05 11:55:28.546000")
    ]

    # Create dataframe for daily activity
    src_event = spark.createDataFrame(eventdata) 

    # Set date variables
    ds_yesterday = "2023-01-04"
    ds_today = "2023-01-05"

    # Run query to process daily activity and create a new record 
    actual_df = do_user_activity_cumulation(spark, src_cumulated, src_device,src_event, ds_yesterday, ds_today)
    
    # Expected results from the processing
    expected_data = [
    UserDevicesCumulated(1178, "BLEXBot",["2023-01-03", "2023-01-01"], "2023-01-05"),  
    UserDevicesCumulated(1409, "Firefox",["2023-01-05"],"2023-01-05"),
    UserDevicesCumulated(1591, "PetalBot", ["2023-01-02"], "2023-01-05"),
    UserDevicesCumulated(3588, "Chrome Mobile", ["2023-01-03"], "2023-01-05"),    
    UserDevicesCumulated(4250, "bingbot",["2023-01-05", "2023-01-04", "2023-01-01"], "2023-01-05"),
    UserDevicesCumulated(7367, "Googlebot", ["2023-01-05","2023-01-03", "2023-01-02", "2023-01-01"], "2023-01-05"),
    UserDevicesCumulated(9446, "Chrome", ["2023-01-05", "2023-01-03","2023-01-01"], "2023-01-05")
    ]

    # Create a dataframe for the expected results
    expected_df = spark.createDataFrame(expected_data)

    # Format dataframe columns
    expected_df = expected_df.withColumn("user_id",col("user_id").cast(StringType()))\
                 .withColumn("device_activity_datelist", col("device_activity_datelist").cast(ArrayType(DateType()))) \
                 .withColumn("date",col("date").cast(DateType())) 

    # Test: compare row counts
    assert actual_df.count() == expected_df.count(), f" Actual: {actual_df.count()} Expected: {expected_df.count()}" 
   
    # Test: compare actual and expected dataframes
    assert_df_equality(actual_df, expected_df,ignore_nullable=True)
    