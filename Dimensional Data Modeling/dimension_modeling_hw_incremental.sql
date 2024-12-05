-- Free Data Engineering Boot Camp
-- Sarah Pfeiffer
-- Dimensional Data Modeling
-- Homework Assignment
--Q5. Incremental query for actors_history_scd
-- write an incremental query that combines the previous year's scd data with new 
-- incoming data from the actors table


-- create type scd_type
create type actor_scd_type as (
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer
);


-- the latest year in actors_scd
with last_year_scd as (
	select * from actors_scd
	where current_year = 1978
	and end_year = 1978
	
)

-- years prior to the latest year in actor_scd

, historical_scd as (
	select 
		actor,
		quality_class,
		is_active,
		start_year,
		end_year
	from actors_scd
	where current_year = 1978
	and end_year < 1978
)

-- new year to add to actors_scd

, this_year_data as (
	select * from actors 
	where current_year = 1979
)

-- records where the 1978 & 1979 values for quality_class and is_active are the same
, unchanged_records as (
	select 
		ts.actor,
		ts.quality_class,
		ts.is_active,
		ls.start_year,
		ts.current_year as end_year
	from this_year_data ts
	join last_year_scd ls
	on ls.actor = ts.actor
	where ts.quality_class = ls.quality_class
	and ts.is_active = ls.is_active
)

-- records where 1978 & 1979 values for quality_class and is_active have changed
, changed_records as (
	select 
		ts.actor,
		unnest(
		array[
			row(
			ls.quality_class,
			ls.is_active,
			ls.start_year,
			ls.end_year
			)::actor_scd_type,
			row(
			ts.quality_class,
			ts.is_active,
			ts.current_year,
			ts.current_year
			):: actor_scd_type
			]) as chgd_records
	from this_year_data ts
	left join last_year_scd ls
	on ls.actor = ts.actor
	where ts.quality_class <> ls.quality_class
	or ts.is_active <> ls.is_active
)

, unnested_changed_records as (
	select actor,
		(chgd_records::actor_scd_type).quality_class,
		(chgd_records::actor_scd_type).is_active,
		(chgd_records::actor_scd_type).start_year,
		(chgd_records::actor_scd_type).end_year
	from changed_records
)

-- new records
,new_records as(
	select 
	ts.actor,
	ts.quality_class,
	ts.is_active,
	ts.current_year as start_year,
	ts.current_year as end_year
	from this_year_data ts
	left join last_year_scd ls
	on ts.actor = ls.actor
	where ls.actor is null
)

select * from historical_scd

union all

select * from unchanged_records

union all

select * from unnested_changed_records

union all

select * from new_records