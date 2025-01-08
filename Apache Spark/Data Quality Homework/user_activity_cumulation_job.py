from pyspark.sql import SparkSession



def do_user_activity_cumulation(spark, dataframe_cumulated, dataframe_devices, dataframe_event, ds_yesterday, ds_today):
    query = f"""
                
            
    with yesterday as (
        select *
        from user_devices_cumulated
        --where date = date('2023-01-14')
        where date = cast('{ds_yesterday}' as date)
    )

    -- Obtain for each user_id by browser_type, event activity
    -- join device and events table to obtain user_id, browser_type, and event_time
    , device_events as (
        select e.user_id, 
            d.browser_type, 
            date(cast(e.event_time as timestamp)) as event_time,
            row_number() over (partition by e.user_id, d.browser_type,date(cast(e.event_time as timestamp)) \
            order by e.user_id, d.browser_type) as row_num
        from devices d 
        join events e
        on d.device_id = e.device_id
        
    )
    , test as (select user_id, row_num from device_events)

    -- reduce to one event record per user_id, browser_type per day
    , device_events_daily as (
        select user_id, 
            browser_type, 
            event_time
        from device_events
        where row_num = 1
        order by user_id, browser_type, event_time
    )

    -- create today CTE as a table with events data for a specific date
    , today as (
        select
            cast(user_id as string) as user_id,	-- change user_id from numeric to text
            browser_type,
            event_time as activity_date		
        from device_events
        --where event_time = date('2023-01-15')
        where event_time = cast('{ds_today}' as date)
        and user_id is not null
        group by user_id, browser_type, event_time
        
    )

    -- create new entry for user_devices_cumulated by joining
    -- today's activity to yesterday's cumulated activity

    select 
        coalesce(t.user_id, y.user_id) as user_id,
        coalesce(t.browser_type, y.browser_type) as browser_type,
        case 
            -- if there is no cumulated activity, add today's activity
            when y.device_activity_datelist is null 
                -- then array[t.activity_date]
                then array(t.activity_date)
            -- if there is no activity today, bring forward the cumulated activity
            when t.activity_date is null			 
                then cast(y.device_activity_datelist as array<date>)
            -- join today's activity to the cumulated activity
            -- else array[t.activity_date] || y.device_activity_datelist
            else array_prepend(cast(y.device_activity_datelist as array<date>),cast(t.activity_date as date))
        end as device_activity_datelist,
        coalesce(cast(t.activity_date as date), cast(y.date as date) + interval '1 DAY') as date

    from today t
    full outer join yesterday y
    on t.user_id = y.user_id
    and t.browser_type = y.browser_type
   

    """    

    dataframe_cumulated.createOrReplaceTempView("user_devices_cumulated")
    dataframe_devices.createOrReplaceTempView("devices")
    dataframe_event.createOrReplaceTempView("events")
    return spark.sql(query)       
         
         
def main():
    ds_yesterday = "2023-01-04"
    ds_today = "2023-01-05"
    
    spark = SparkSession.builder \
      .master("local") \
      .appName("user_activity_cumulation") \
      .getOrCreate()
    output_df = do_user_activity_cumulation(spark, spark.table("user_devices_cumulated"), spark.table("devices"), spark.table("events"), ds_yesterday, ds_today)
    output_df.write.mode("overwrite").insertInto("user_devices_cumulated")  