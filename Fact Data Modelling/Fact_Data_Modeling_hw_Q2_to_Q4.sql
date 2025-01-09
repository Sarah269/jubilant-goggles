-- DataExpert.io Academy Boot Camp
-- Fact Data Modeling Homework
-- Q2 to Q4



-- Q2. DDL for an 'user_devices_cumulated' table that has:
-- a 'device_activity_datelist' which tracks a user's active days by 'browser type'



create table user_devices_cumulated (
	user_id text,
	browser_type text,
	device_activity_datelist date[],
	date date,
	primary key (user_id, browser_type, date)
);

-- Q3. Cumulative query to generate device_activity_date_list from events 

-- Populate user_devices_cumulated with 14 days of event day

insert into user_devices_cumulated
-- create yesterday CTE as a table with the user_devices_cumulated data and filter on a specific date
with yesterday as (
	select *
	from user_devices_cumulated
	where date = date('2023-01-14')
)

-- Obtain for each user_id by browser_type, event activity
-- join device and events table to obtain user_id, browser_type, and event_time
, device_events as (
	select e.user_id, 
		d.browser_type, 
		date(cast(e.event_time as timestamp)) as event_time,
		row_number() over (partition by user_id, browser_type,date(cast(e.event_time as timestamp))) as row_num
	from devices d 
	join events e
	on d.device_id = e.device_id
	
)

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
		cast(user_id as text) as user_id,	-- change user_id from numeric to text
		browser_type,
		event_time as activity_date		
	from device_events
	where event_time = date('2023-01-15')
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
			then array[t.activity_date]
		-- if there is no activity today, bring forward the cumulated activity
		when t.activity_date is null			 
			then y.device_activity_datelist
		-- join today's activity to the cumulated activity
		else array[t.activity_date] || y.device_activity_datelist
	end as device_activity_datelist,
	coalesce(t.activity_date, y.date + interval '1 DAY') as date

from today t
full outer join yesterday y
on t.user_id = y.user_id
and t.browser_type = y.browser_type

	

-- Q4. 'datelist_int' generation query. 
-- Convert the device_activity_datelist column into a 'datelist_Int' column

-- list of dates users were active
with users as (
	select *
	from user_devices_cumulated
	where date = date('2023-01-14')
	
)
	
-- generate a series of dates from 2023-01-01 to 2023-01-31
	
, series as (
	select *
	from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 DAY') as series_date
)

-- cross join users with series

, activity_series as (
	select *
	from users 
	cross join series 
	order by user_id, series_date
	
)

-- convert device_activity_datelist into integers values

, is_activity_date_in_series_date as (
	select 
		case when 
			device_activity_datelist @> array[date(series_date)]		-- Is datelist value in series_date?
			-- converts dates into integer values that are power of 2
			then cast(POW(2, 32 - (date - date(series_date))) as bigint)	-- number of days between date and series_date
			else 0
		end  as datelist_int,
		*
	from activity_series
)

select user_id,
	browser_type,
	-- convert datelist_int into a bit string
	cast(cast(sum(datelist_int) as bigint) as bit(32)) as datelist_int
from is_activity_date_in_series_date
group by user_id, browser_type;
	