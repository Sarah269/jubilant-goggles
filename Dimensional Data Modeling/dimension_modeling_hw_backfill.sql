-- Free Data Engineering Boot Camp
-- Sarah Pfeiffer
-- Dimensional Data Modeling
-- Homework Assignment
-- Q3. DDL for actors_history_scd table
-- Q4. Backfill query for actors_history_scd
-- write a backfill query that can populate the entire actors_history_scd table in a single query
-- actors table has data from 1970 to 1980

--drop table actors_scd;

create table actors_scd (
	actor text,
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer,
	current_year integer,
	primary key(actor, start_year)
	);

 insert into actors_scd

-- use lag to get the previous value on the same record
with with_previous as (
	select actor,
	current_year,
	quality_class,
	is_active,
	lag(quality_class,1) over (partition by actor order by current_year) as prev_quality_class,
	lag(is_active,1)  over (partition by actor order by current_year) as prev_is_active
	from actors
	where current_year <= 1978
	)

-- compare the prev value to the current value, if the same no change, if not change
, with_indicators as (
	select *,
	case
		when quality_class <> prev_quality_class then 1
		when is_active <> prev_is_active then 1
		else 0
	end as change_indicator
	from with_previous
)

-- sum the number of changes in quality_class or is_active
,with_streaks as (
	select *,
		sum(change_indicator) over (partition by actor order by current_year) as streak_identifier
	from with_indicators
)
	
select actor,
quality_class,
is_active,
min(current_year) as start_year,
max(current_year) as end_year,
1978 as current_year
from with_streaks
group by actor, streak_identifier, is_active, quality_class
order by actor, streak_identifier;

select * from actors_scd;
