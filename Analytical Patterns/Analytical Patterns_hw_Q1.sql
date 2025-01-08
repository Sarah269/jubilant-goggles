-- DataExpert.io Academy Data Engineering Bootcamp
-- Analytical Patterns Homework

--  Q1. A query that does state change tracking for `players`
--  - A player entering the league should be `New`
--  - A player leaving the league should be `Retired`
--  - A player staying in the league should be `Continued Playing`
--  - A player that comes out of retirement should be `Returned from Retirement`
--  - A player that stays out of the league should be `Stayed Retired`

-- Create player status change tracking table

create table player_sct (
	player_name text,
	first_active_season integer,
	last_active_season integer,
	is_active boolean,
	years_since_last_season integer,
	season_active_status text,
	seasons_active integer[],
	current_season integer,
	primary key (player_name, current_season)
	);



insert into player_sct 

-- historical activity
with yesterday as (
	select *
	from player_sct
	where current_season = 2004
)

-- Current activity
, today as (
	select 
		player_name,
		years_since_last_season,
		current_season,
		is_active
		from players 
		where current_season = 2005
		
		
)

-- Status Change Tracking for player status
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(y.first_active_season, t.current_season) as first_active_season,
	-- Determine the last season the player was active
	case
		when y.current_season is null then t.current_season
		when t.current_season is null then y.last_active_season
		else t.current_season - t.years_since_last_season
		
	end as last_active_season,
	
	coalesce(t.is_active, y.is_active) as is_active,
	coalesce(t.years_since_last_season, y.years_since_last_season + 1) as years_since_last_season,
	-- Determine change status categor for player active status
	case 
		when y.is_active is null then 'New'
		when t.is_active = false and y.is_active = true then 'Retired'
		when t.is_active = true and y.is_active = true then 'Continued Playing'
		when t.is_active = true and y.is_active = false then 'Returned from Retirement'
		when t.is_active = false and y.is_active = false then 'Stay Retired'
		
		else 'unknown'
	end as season_active_state,
	-- Populate seasons_active with the years the player was active
	coalesce(y.seasons_active, array[]::integer[])	|| 
		case 
	 		when t.is_active then array[t.current_season]
 			else array[]::integer[]
 		end as seasons_active,
 	coalesce(t.current_season, y.current_season + 1) as current_season
	
	
from today t
full outer join yesterday y
on t.player_name = y.player_name

-- Sampling of players.  Michael Jordan's data has all of the player active statuses
select *
from player_sct
where current_season = 2004
and player_name in ('Michael Jordan', 'Alton Lister', 'Alvin Sims', 'Aaron Williams')
	