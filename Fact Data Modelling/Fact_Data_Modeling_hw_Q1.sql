-- DataExpert.io Academy Boot Camp
-- Fact Data Modeling Homework

-- Q1. query to deduplicate 'game_details' from Day 1 so there's no duplicates

-- Identify duplicates in game_details

with game_details_identify_dupes as (
-- assign a row number to records with the same game_id, team_id, player_id
select *,
	row_number() over (partition by game_id, team_id, player_id order by player_name) as row_num
	
from game_details
)

-- Drop the duplicate records

, game_details_deduped as (
--  only select the first record for game_id, team_id, player_id thus dropping duplicate records
select *
from game_details_identify_dupes 
where row_num = 1
)

-- game details deduped
select *
from game_details_deduped;
