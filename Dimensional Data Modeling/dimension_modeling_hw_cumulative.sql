-- Free Data Enginnering Boot Camp
-- Sarah Pfeiffer
-- Dimensional Data Modeling
-- Homework Assignment
-- Q1. Create DDL for actors table
-- Q2. Create cumulative table generation query. Write a query that populates the actors table one year at a time

-- drop type films;

-- create an array of struct for film attributes
create type films as (
	year integer,
	film text,
	votes integer,
	rating real,
	filmid text
	);

-- create enum for actor's performance quality
create type quality_class as enum ('star', 'good', 'average', 'bad');

--drop table actors;

--create table actors (
--	actorid text,
--	actor text,
--	current_year integer,
--	films films[],
--	quality_class quality_class,
--	is_active boolean
--	);

-- inital current year = 1969, year = 1970
-- 	min(year) = 1970, max(year) = 2021

insert into actors 
with  actor_films_with_avgrating as (
     	select *, 
		avg(rating) over (partition by actorid, year order by actorid, year) as avg_rating
		from actor_films
		order by actorid, year
)
     
	,yesterday as (
	    select * from actors
	    where current_year = 1980
)
     ,today as (
         select * from actor_films_with_avgrating
         where year = 1981
)
 
	,prelim_actor as (     
	select 
		coalesce(t.actor, y.actor) as actor,
		coalesce(t.actorid, y.actorid) as actorid,
		case when y.films is null
			then array[row(t.year,
						   t.film,
						   t.votes,
						   t.rating,
						   t.filmid)::films]
			when t.year is not null then y.films ||
				  array[row(t.year,
						   t.film,
						   t.votes,
						   t.rating,
						   t.filmid)::films]
			else y.films
			end as films,
			
			case 
				when t.year is not null then
					case when t.avg_rating > 8 then 'star'
					     when t.avg_rating > 7 and t.avg_rating <= 8 then 'good'
					     when t.avg_rating > 6 and t.avg_rating <= 7 then 'average'
					     when t.avg_rating <= 6 then 'bad'
					     
					end :: quality_class
				else y.quality_class
			end as quality_class,
			case 
				when t.year is not null then true 
				else false 
			end as is_active,
				
			
			coalesce(t.year, y.current_year + 1) as current_year
	from today t 
	full outer join yesterday y
	on t.actorid = y.actorid

)

-- Create one row per actor for the current_year even if the actor has multiple films
-- unnest films array
-- aggregate films array

, prelim_actor_unnest as (
		select  actor, 
		actorid,
		current_year,
		unnest(films) as films, 
		quality_class,
		is_active
		from prelim_actor
		 
		
)


select actorid, 
actor, 
current_year, 
array_agg(row((films::films).year, (films::films).film , (films::films).votes, (films::films).rating, (films::films).filmid)::films order by (films::films).year desc) as films,
quality_class, 
is_active
from prelim_actor_unnest
group by actor, actorid, current_year, quality_class, is_active
order by actorid, current_year;

		
select * from actors
where  current_year = 1981
order by actor;


 
 