#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# DataExpert.io Data Engineering Academy
# Apache Spark Assignment 1
# File #1
# Build a Spark job that
# - Disabled automataic broadcast join
# - Explicit broadcast join
# - Bucket join


# In[ ]:


# Import Libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf


# In[ ]:


# Create Spark Session
spark = SparkSession.builder.appName('practice2').getOrCreate()


# 
# 

# In[ ]:


# Requirement #1: Disable automatic broadcast join
# Disable broadcast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


# In[ ]:


# Read csv files into pyspark dataframes

# Read matches.csv
# a row for every match

matches = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/matches.csv")
# Read match_details.csv
# a row for every players' performance in a match

matchDetails =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/match_details.csv")
# Read medals_matches_players
# a row for every medal type a player gets in a match

medalsMatchesPlayers =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals_matches_players.csv")
# Read medals
# a row for every medal type

medals =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals.csv")
# Read maps
# a row for every map type

maps =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/maps.csv")


# In[ ]:


# Rename columns

medalsMatchesPlayers = medalsMatchesPlayers.withColumnRenamed("count","medal_count")

maps = maps.withColumnRenamed("name","map_name")

medals = medals.withColumnRenamed("classification", "medal_class") \
        .withColumnRenamed("description","medal_desc") \
        .withColumnRenamed("difficulty","medal_difficulty") \
        .withColumnRenamed("name", "medal_name")


# In[ ]:


# Requirement #2: Broadcast joins
# import broadcast
from pyspark.sql.functions import expr, col, broadcast, split



# In[ ]:


# Explicitly broadcast join maps to matches
# Select columns from matches
# Select columns from maps
# Join on mapid

matchesMaps = (
    matches
    .select('match_id', 'is_team_game', 'playlist_id', 'completion_date', 'mapid')  \
    .join(broadcast(maps).select('mapid', 'name'), on='mapid', how='left_outer')  \
    .select('match_id', 'is_team_game', 'playlist_id', 'completion_date', 'mapid', 'map_name')  \
)


# In[ ]:


# Explicitly broadcast joins medals to medalMatchesPlayers
# Select columns from medalsMatchesPlayers
# Select columns from medals
# Join on medal_id

mmpMedals = (
    medalsMatchesPlayers
    .select('match_id', 'player_gamertag','medal_id','medal_count') \    
    .join(broadcast(medals).select('medal_id','medal_name'), \
          on='medal_id' , how='left_outer') \
    .select('match_id', 'player_gamertag','medal_id','medal_name', 'medal_count') \
    )


# In[ ]:


#  Create a version of matchDetails with fewer columns.  
matchDetailsMini = matchDetails.select("match_id","player_gamertag", "player_total_kills", "player_total_deaths")                


# In[ ]:


# Requirement #3 Bucket join matches, match_details, and medals_matches_players on match_id with 16 buckets


# In[ ]:


# Create table for matches

matchesMaps.write.format("iceberg").mode("overwrite").bucketBy(16,"match_id").saveAsTable("bootcamp.matchesMaps_bucketed")


# In[ ]:


# Create table with 16 buckets in iceberg format for medals_matches_players

mmpMedals.write.format("iceberg").mode("overwrite").bucketBy(16,"match_id").saveAsTable("bootcamp.medals_bucketed")


# In[ ]:


# Create table with 16 bucketes in iceberg format for match_details

matchDetailsMini.write.format("iceberg").mode("overwrite").bucketBy(16,"match_id").saveAsTable("bootcamp.matchDetails_bucketed")


# In[ ]:


# Set the following to ensure no shuffling
spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true')
spark.conf.set('spark.sql.sources.v2.bucketing.pushPartValues.enabled','true')
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping','true')
spark.conf.set('spark.sql.requireAllClusterKeysForCoPartition','false')


# In[ ]:


# Bucket join bootcamp.matchesMaps_bucketed with bootcamp.matchDetails_bucketed
# Select all column from bootcamp.matchesMaps_bucketed
# Select three columns from bootcamp.matchDetails_bucketed
# Join on match_id

matchesJoinDetails = spark.sql("""  
select
mm.*, 
md.player_gamertag, 
md.player_total_kills, 
md.player_total_deaths 
from bootcamp.matchesMaps_bucketed mm
join bootcamp.matchDetails_bucketed md 
on mm.match_id = md.match_id 
 """)


# In[ ]:


# Create table with 16 buckets in iceberg format for matchesJoinDetails

matchesJoinDetails.write.format("iceberg").mode("overwrite").bucketBy(16,"match_id").saveAsTable("bootcamp.matches_n_Details_bucketed")


# In[ ]:


# Bucket Join bootcamp.matches_n_Details_bucketed with bootcamp.medals_bucketed
# Select all columns from bootcamp.matches_n_Details_bucketed
# Select three columns from bootcamp.medals_bucketed
# Join on match_id and player_gamertag

gamingDetails1 = spark.sql(""" 
select 
md.*, 
m.medal_id, 
m.medal_name,
m.medal_count
from bootcamp.matches_n_Details_bucketed md
left join bootcamp.medals_bucketed m 
on md.match_id = m.match_id
and md.player_gamertag = m.player_gamertag 

 """)


# In[ ]:


# Create table with 16  buckets in iceberg format for gamingDetails1

gamingDetails1.write.format("iceberg").mode("overwrite").bucketBy(16,"match_id").saveAsTable("bootcamp.gamingInfo")

