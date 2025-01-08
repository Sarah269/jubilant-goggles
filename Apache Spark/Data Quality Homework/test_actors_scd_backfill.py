from chispa.dataframe_comparer import *
from ..jobs.actors_scd_backfill_job import do_actor_scd_transformation
from collections import namedtuple
from pyspark.sql.types import *

ActorYear = namedtuple("ActorYear", "actor actorid current_year quality_class is_active")
ActorScd = namedtuple("ActorScd", "actor actorid quality_class is_active start_year end_year current_year")


def test_scd_generation(spark):
    source_data = [
        ActorYear("Meat Loaf", "10", 1974, 'Good', True),
        ActorYear("Meat Loaf", "10", 1975, 'Good', True),
        ActorYear("Meat Loaf", "10", 1976, 'Bad', True),
        ActorYear("Meat Loaf", "10", 1977, 'Bad', True),
        ActorYear("Skid Markel", "15", 1976, 'Bad', True),
        ActorYear("Skid Markel", "15", 1977, 'Bad', True)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Meat Loaf", "10", 'Good', True, 1974, 1975, 1977),
        ActorScd("Meat Loaf", "10" ,'Bad', True, 1976, 1977, 1977 ),
        ActorScd("Skid Markel","15", 'Bad', True, 1976, 1977, 1977)
    ]

    expected_schema = StructType([\
        StructField("actor",StringType(),True), \
        StructField("actorid", StringType(), True),\
        StructField("quality_class", StringType(), True),\
        StructField("is_active",BooleanType(), True),\
        StructField("start_year",LongType(), True),\
        StructField("end_year", LongType(), True),\
        StructField("current_year",IntegerType(), False)
    ])

    
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)