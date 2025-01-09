-- DataExpert.io Academy Boot Camp
-- Fact Data Modeling Homework
-- Q5 and Q6


-- Q5. DDL for hosts_cumulated table. 


create table hosts_cumulated (
	host text,
	month_start date,
	host_activity_datelist date[],
	primary key (host, month_start)
);


-- Q6. Incremental query to generate host_activity_datelist
	
insert into hosts_cumulated
-- from events select host and days the host had activity
-- Identify multiple records for the same activity date
with unique_event_time as (
	select host, 
	date(cast(event_time as timestamp)) as event_time,
	row_number() over (partition by host, date(cast(event_time as timestamp)) order by date(cast(event_time as timestamp))) as row_num
	from events 
	where date(event_time) = date('2023-01-23')
)

-- drop duplicate dates
, host_activity_today as (
	select host, 
	event_time
	from unique_event_time
	where row_num = 1
)

-- cumulated host activity
, yesterday_hosts_cumulated as (
	select *
	from hosts_cumulated
	where month_start = date('2023-01-01')
)

-- Add today's host activity to hosts_cumulated
select 
	coalesce(t.host, y.host) as host,
	coalesce(y.month_start, date((date_trunc('month', event_time)))) as month_start,
	case 
		when t.host is null 	-- if host has no activity today
			then y.host_activity_datelist
		when y.host_activity_datelist is not null 	-- if host has activity
			then y.host_activity_datelist || t.event_time
		 when y.host_activity_datelist is null		-- add activity for a new host, month_start
			then array[t.event_time]
	end as host_activity_datelist
	
	
from host_activity_today t
full outer join yesterday_hosts_cumulated y
on t.host = y.host
-- if host, month_start exists, update host_activity_datelist
-- otherwise, insert new host, month_start
on conflict (host,month_start)
do update set host_activity_datelist = excluded.host_activity_datelist;


