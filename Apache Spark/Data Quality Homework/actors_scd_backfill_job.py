from pyspark.sql import SparkSession

query = """

with with_previous as (
	select actor,
    actorid,
	current_year,
	quality_class,
	is_active,
	lag(quality_class,1) over (partition by actor order by current_year) as prev_quality_class,
	lag(is_active,1)  over (partition by actor order by current_year) as prev_is_active
	from actors
	where current_year < 1978
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
actorid,
quality_class,
is_active,
min(current_year) as start_year,
max(current_year) as end_year,
1977 as current_year
from with_streaks
group by actor 
, actorid
, streak_identifier
, is_active
, quality_class
order by actor
, streak_identifier

"""


def do_actor_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd") \
        .getOrCreate()
    output_df = do_actor_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")