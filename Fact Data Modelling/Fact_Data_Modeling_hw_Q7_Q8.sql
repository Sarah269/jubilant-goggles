-- DataExpert.io Academy Boot Camp
-- Fact Data Modeling Homework
-- Q7 and Q8

-- Q7. DDL for reduced fact table 'host_activity_reduced'

create table host_activity_reduced (
	host text,
	month date,
	hits_array real [],
	unique_visitors_array real[],
	primary key (host, month)
);


-- Q8. increamental query that loads 'host_activity_reduced' day-by-day

insert into host_activity_reduced
-- aggregate host activity
with agg_daily_activity as (
	select host, 
		date(cast(event_time as timestamp)) as event_time, 
		count(1) as hits, 
		count(distinct user_id) as visitors
	from events  
	where date(cast(event_time as timestamp)) = date('2023-01-01')
	group by host, date(cast(event_time as timestamp))
)

-- host activity to-date
, yesterday as (
	select *
	from host_activity_reduced
	where month = date('2023-01-01')
)

select 
	coalesce(t.host, y.host) as host,
	coalesce(y.month, t.event_time) as month,
	
	-- update hits_array. hits_array is null, pre-fill with 0's and concatenate hits
	-- if hits_array is not null, concatenate hits_array with today's hits
	
	case when y.hits_array is null
			then array_fill(0, array[coalesce(event_time- date(date_trunc('month',event_time)),0)]) || array[coalesce(t.hits,0)]
		 when y.hits_array is not null
		 	then y.hits_array || array[coalesce(t.hits,0)]
		 end as hits_array,
		 
	-- update unique_visitorss_array. unique_visitors_array is null, pre-fill with 0's and concatenate hits
	-- if unique_visitors_array is not null, concatenate unique_visitors_array with today's visitors
		 
	case when y.unique_visitors_array is null
			then array_fill(0, array[coalesce(event_time- date(date_trunc('month',event_time)),0)]) || array[coalesce(t.visitors,0)]
		 when y.unique_visitors_array is not null
		 	then y.unique_visitors_array || array[coalesce(t.visitors,0)]
		 end as unique_visitors_array
from agg_daily_activity t
full outer join host_activity_reduced y
on t.host = y.host
-- if a row exists with (host, month) then update hits_array and unique_visitors_array
-- if a row does not exist with (host, month) then insert
on conflict (host, month)
do 
	update set hits_array = excluded.hits_array, unique_visitors_array = excluded.unique_visitors_array;


