-- DataExpert.io Academy Data Engineering Bootcamp
-- Analytical Patterns Homework
-- Q2 and Q3


--Q2.  A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data

-- create table to store query results

create table game_details_grouping as (

-- join games and game_details.  Select and/or derive columns
with game_details_augmented as (
select 
g.game_date_est as game_date, 
coalesce(g.season, '0') as season,
g.home_team_wins, g.home_team_id, g.visitor_team_id,
-- Derive game outcome from home_team_wins. 1 = yes, 0 = no
case
	when g.home_team_wins = 1 then g.home_team_id
	else g.visitor_team_id
end as game_winner,

gd.game_id,
gd.team_id,
coalesce(gd.team_abbreviation,'unknown') as team_abbreviation,
coalesce(gd.player_name,'unknown') as player_name,
coalesce(gd.pts,0) as pts
from game_details gd
join games g
on gd.game_id = g.game_id
order by game_id

)

-- Using grouping sets to get multiple aggregations at once
select 
-- identify aggregation levels. 0 = aggregation turned on
	case
		when grouping(season) = 0 and grouping(player_name) = 0 and grouping(team_abbreviation) = 0 then 'season_player_team'
		when grouping(season) = 0 and grouping(player_name) = 0 then 'season_player'
		when grouping(player_name) = 0 and grouping(team_abbreviation) =0 then 'player_team'
		when grouping(team_abbreviation)=0 then 'team'
	end as aggregation_level,
	season,
	player_name,
	team_abbreviation,
	count(distinct game_id) as num_games,
	sum(pts) as total_pts
from game_details_augmented
group by grouping sets(
	(season, team_abbreviation, player_name),
	(player_name,team_abbreviation),
	(season, player_name),
	(team_abbreviation)
	)
order by sum(pts) desc
);


-- Who scored the most points playing for one team? Giannis Antetokounmpo. 15,591 points.

select * 
from game_details_grouping
where aggregation_level = 'player_team'
order by total_pts desc
limit 1;


-- Who scored the most points in one season? James Harden. 3247 points

select * 
from game_details_grouping
where aggregation_level = 'season_player'
order by total_pts desc
limit 1;


-- Which team has won the most games? GSW. 445 games.

with game_details_team as (
select 
g.game_date_est as game_date, 
coalesce(g.season, '0') as season,
-- Derive game outcome from home_team_wins. 1 = yes, 0 = no
case
	when g.home_team_wins = 1 then 
		case when g.home_team_id=team_id then 'won' 
	else 'lost' end
	when g.home_team_wins = 0 then
		case when g.visitor_team_id = team_id then 'won'
		else 'lost' end
end as game_outcome,

gd.game_id,
gd.team_id,
coalesce(gd.team_abbreviation,'unknown') as team_abbreviation,

sum(gd.pts) as game_points
from game_details gd
join games g
on gd.game_id = g.game_id
group by game_date, season, game_outcome, gd.game_id, team_id, team_abbreviation
order by game_id

)

-- Using grouping sets to get multiple aggregations at once
-- game_details_team_grouping 
, game_details_team_grouping as (
select 
-- identify aggregation levels. 0 = aggregation turned on
	case
		when grouping(season) = 0 and grouping(team_abbreviation) = 0 then 'season_team'
		when grouping(season) = 0 then 'season'
		when grouping(team_abbreviation)=0 then 'team'
	end as aggregation_level,

season,
team_abbreviation,
count(game_id) as games_played,
-- count games won
sum(case when game_outcome = 'won' then 1 else 0 end) as games_won,
-- count games lost
sum(case when game_outcome = 'lost' then 1 else 0 end) as games_lost, 
sum(game_points) as game_points
from game_details_team
group by grouping sets(
	(season, team_abbreviation),
	(season),
	(team_abbreviation)
	)
order by games_won desc
)

select *
from game_details_team_grouping
where aggregation_level = 'team'
order by games_won desc
limit 1;

-- Q3. A query that uses window functions on `game_details` to find out the following things:

-- What is the most games a team has won in a 90 game stretch?  GSW - 77 games.

-- join games and game_details.  Select and/or derive columns

with game_details_team as (
select 
g.game_date_est as game_date, 
coalesce(g.season, '0') as season,
case
	when g.home_team_wins = 1 then 
		case when g.home_team_id=team_id then 'won' 
	else 'lost' end
	when g.home_team_wins = 0 then
		case when g.visitor_team_id = team_id then 'won'
		else 'lost' end
end as game_outcome,

gd.game_id,
gd.team_id,
coalesce(gd.team_abbreviation,'unknown') as team_abbreviation,

sum(gd.pts) as game_points
from game_details gd
join games g
on gd.game_id = g.game_id
group by game_date, season, game_outcome, gd.game_id, team_id, team_abbreviation
order by game_id

)

-- using window function construct 90 game window

, rolling_90 as (
select team_abbreviation, 
row_number() over (partition by team_abbreviation order by team_abbreviation, game_date) > 90 as complete_window,
case when row_number() over (partition by team_abbreviation order by team_abbreviation, game_date) > 90 is true then
sum(case when game_outcome = 'won' then 1 else 0 end) over (partition by team_abbreviation
															order by team_abbreviation, game_date
															rows between 89 preceding and current row) 
end as number_games_won
from game_details_team
order by team_abbreviation
)

-- Filter on complete windows, find the maximum number of games won in a rolling 90 game window

select team_abbreviation, max(number_games_won) as most_games_won_in_90
from rolling_90
where complete_window is true
group by team_abbreviation
order by most_games_won_in_90 desc
limit 1;

-- How many games in a row did LeBron James score over 10 points a game? 62

-- join games and game_details.  Select and/or derive columns

with game_details_augmented as (
select 
g.game_date_est as game_date, 
g.season,

-- Derive game outcome from home_team_wins. 1 = yes, 0 = no
case
	when g.home_team_wins = 1 then g.home_team_id
	else g.visitor_team_id
end as game_winner,

gd.game_id,
gd.team_id,
gd.team_abbreviation,
gd.player_name,
coalesce(gd.pts,0) as pts
from game_details gd
join games g
on gd.game_id = g.game_id
order by game_id
)

-- Filter on LeBron James and create flag to identify games where he scored over 10pts
, LeBron_points as (
select game_date, player_name, pts, 
case when pts > 10 then true else false end as over_10pts

from game_details_augmented
where player_name = 'LeBron James'
order by game_date
)

-- create rn1 and rn2 to help with identifying consecutive runs of scoring over 10pts
, prep_for_ranking_pts as (
select *, 
roW_NUMBER() OVER (PARTITION BY player_name ORDER BY game_date ) as rn1,
ROW_NUMBER() OVER (PARTITION BY player_name, over_10pts ORDER BY game_date ) as rn2
from LeBron_points
order by game_date
)

-- compute consecutive games resetting when there is a break in scoring over 10 pts
, identify_consecutive_games as (
select *, 
ROW_NUMBER() OVER (PARTITION BY player_name, over_10pts, rn1 - rn2
                          ORDER BY game_date ) AS RN
from prep_for_ranking_pts
order by game_date
)

-- Find the longest streak of scoring over 10 pts in a game
select player_name, max(rn) as longest_streak_over10pts
from identify_consecutive_games
group by player_name





