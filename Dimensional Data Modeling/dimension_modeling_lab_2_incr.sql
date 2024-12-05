-- although this query is long it is processing a lot less data
-- it is processing compacted 2021 and 2022 data
-- it is processing probably 20x less data
-- more complicated because you have to think of all the ways things can changed
-- this is a powerful query

-- assumption:  assumed is_active and scoring_class are never null. if null breaks stuff
-- have assumptions checked when building query
-- it had a sequentiall problem making it harder to backfill


--create type scd_type as (
--   				scoring_class scoring_class,
--   				is_active boolean,
--   				start_season integer,
--   				end_season integer
--   				
--   				);



with last_season_scd as (
    select * from public.players_scd
    where current_season = 2021
    and end_season = 2021
),

    historical_scd as (
    select 
       player_name,
       scoring_class,
       is_active,
       start_season,
       end_season
    from public.players_scd
    where current_season = 2021
    and end_season < 2021
),
this_season_data as (
    select * from players 
    where current_season = 2022
    ),
 unchanged_records as (
     select 
            ts.player_name,
            ts.scoring_class,
            ts.is_active,
            ls.start_season,
            ts.current_season as end_season
      from this_season_data ts
      join last_season_scd ls
      on ls.player_name = ts.player_name
      where ts.scoring_class = ls.scoring_class
      and ts.is_active = ls.is_active
   ),
  changed_records as (
      select 
            ts.player_name,
            unnest(
            array[
                row(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                ls.end_season
               ):: scd_type,
                
                row(
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
                
                ):: scd_type
                ]) as records
      from this_season_data ts
      left join last_season_scd ls
      on ls.player_name = ts.player_name
      where ts.scoring_class <> ls.scoring_class
      or ts.is_active <> ls.is_active
    
   ),
   unnested_changed_records as (
       select player_name,
              (records::scd_type).scoring_class,
              (records::scd_type).is_active,
   			  (records::scd_type).start_season,
   			  (records::scd_type).end_season
   			  from changed_records
   	),
   	new_records as (
   	  select 
   	  ts.player_name,
   	  ts.scoring_class,
   	  ts.is_active,
   	  ts.current_season as start_season,
   	  ts.current_season as end_season
   	  from this_season_data ts
   	  left join last_season_scd ls
   	  on ts.player_name = ls.player_name
   	  where ls.player_name is null 
   	  
   	)
   	
select * from historical_scd

union all

select * from unchanged_records

union all

select * from unnested_changed_records

union all

select * from new_records


     
     