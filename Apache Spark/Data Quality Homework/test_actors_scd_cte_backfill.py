# DataExpert.io Academy Data Engineering Bootcamp
# Pytest Assignment
# Create tests with fake input and expected output data
# Tests for actor_scd CTEs

from pyspark.sql import SparkSession
from pyspark import SparkConf
from collections import namedtuple
import pytest

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, sum, expr

# Create Spark Session
spark = SparkSession.builder.appName('practice3').getOrCreate()

@pytest.fixture
def source_df():
    ActorYear = namedtuple("ActorYear", "actor actorid current_year quality_class is_active")
    source_data = [
        ActorYear("Meat Loaf", "10", 1974, 'Good', True),
        ActorYear("Meat Loaf", "10", 1975, 'Good', True),
        ActorYear("Meat Loaf", "10", 1976, 'Bad', True),
        ActorYear("Meat Loaf", "10", 1977, 'Bad', True),
        ActorYear("Skid Markel", "15", 1976, 'Bad', True),
        ActorYear("Skid Markel", "15", 1977, 'Bad', True)
    ]
    yield spark.createDataFrame(source_data)

   
@pytest.fixture
def with_prev(source_df):
    windowSpec = Window.partitionBy("actorid").orderBy("current_year")
    return source_df.withColumn("prev_quality_class", lag("quality_class", 1).over(windowSpec)) \
        .withColumn("prev_is_active", lag("is_active", 1).over(windowSpec))

 
@pytest.fixture
def with_chg_ind(with_prev):
    return with_prev.withColumn(
        "change_indicator",
        expr("CASE WHEN quality_class <> prev_quality_class THEN 1 WHEN is_active <> prev_is_active THEN 1 ELSE 0 END")
    )

def test_with_previous(source_df):
    assert source_df.filter(source_df.current_year >= 1978).count() == 0

def test_with_indicators(with_chg_ind):
  #   # set partition by and order by for window functions
  #   windowSpec = Window.partitionBy("actorid").orderBy("current_year")

  #   # Add prev_quality_class and prev_is_active columns to source data to create with_previous
  #   with_prev = source_df.withColumn("prev_quality_class",lag("quality_class",1).over(windowSpec)) \
	 # .withColumn("prev_is_active",lag("is_active",1).over(windowSpec))
    
  #   # create with_indicators
  #   with_chg_ind = with_prev.withColumn("change_indicator",expr("CASE when quality_class <> prev_quality_class THEN 1 when is_active <> prev_is_active THEN 1 ELSE 0 END AS change_indicator"))
    
    # Create a view for with_chg_ind
    with_chg_ind.createOrReplaceTempView("with_chg_ind_v")

    # Count the number of rows with a correct logic calculation for change indicator
    # Identified three main scenarios
    # 1 indicates the change_indicator was set correctly
    # 0 indicates the change_indicator was not set correctly
    result = spark.sql("""
    with CTE1 as (
    select *,
    case 
        when (quality_class = prev_quality_class) = true and (is_active = prev_is_active) = true
               and (change_indicator = 0) then 1 
               
        when ((prev_quality_class is null) = true or (prev_is_active is null) = true)
               and (change_indicator = 0) then 1  
               
         when ((quality_class <> prev_quality_class) = true or (is_active <> prev_is_active) = true)
               and (change_indicator = 1) then 1 
        else 0 end as chk_chg_ind
    from with_chg_ind_v
    )
    select sum(chk_chg_ind) as chg_ind_logic_correct  from CTE1
    """)

    # Fetch the result
    chg_ind_logic_correct = result.collect()[0]["chg_ind_logic_correct"]
    
    # Check whether all of the rows have a correct calculation
    assert chg_ind_logic_correct == with_chg_ind.count(), f"Expected chg_ind_logic_correct to be {with_chg_ind.count()}, but got {chg_ind_logic_correct}"

def test_with_streaks(with_chg_ind):
  #   # set partition by and order by for window functions
  #   windowSpec = Window.partitionBy("actorid").orderBy("current_year")

  #   # Add prev_quality_class and prev_is_active columns to source data to create with_previous
  #   with_prev = source_df.withColumn("prev_quality_class",lag("quality_class",1).over(windowSpec)) \
	 # .withColumn("prev_is_active",lag("is_active",1).over(windowSpec))
    
  #   # create with_indicators
  #   with_chg_ind = with_prev.withColumn("change_indicator",expr("CASE when quality_class <> prev_quality_class THEN 1 when is_active <> prev_is_active THEN 1 ELSE 0 END AS change_indicator"))
    
    # Create a view for with_chg_ind
    with_chg_ind.createOrReplaceTempView("with_indicators")
    
    # Query to validate calculation streak_indicator
    result = spark.sql("""
    WITH with_streaks AS (
        SELECT *,
            SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
        FROM with_indicators
    )
    -- Add lag columns for actorid and streak_identifier
    ,with_validations AS (
        SELECT *,
            LAG(streak_identifier) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_streak_identifier,
            LAG(actorid) OVER (ORDER BY actorid, current_year) AS prev_actorid
        FROM with_streaks
    )
    -- Add expected streak identifier
    SELECT 
        actorid, 
        current_year, 
        change_indicator, 
        streak_identifier, 
        prev_streak_identifier,
        prev_actorid,
        -- Generate the expected streak identifier
        CASE 
            WHEN prev_actorid IS NULL THEN change_indicator 
            WHEN actorid != prev_actorid THEN change_indicator 
            WHEN change_indicator = 0 THEN prev_streak_identifier 
            WHEN change_indicator = 1 THEN prev_streak_identifier + change_indicator 
        END AS expected_streak_identifier
    FROM with_validations
    """)
    
    # Compare streak_identifier to expected_streak_identifier
    for row in result.collect():
        assert row["streak_identifier"] == row["expected_streak_identifier"], (
            f"Mismatch in streak identifier validation: {row}"
        )




    