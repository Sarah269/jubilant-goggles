#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# DataExpert.io Data Engineering Academy
# Apache Spark Assignment 1
# File #2
# Aggregate joined data 
# Answer four queries
# Try different .SortWithinPartitions


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


# Disable broadcast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


# In[ ]:


# import broadcast
from pyspark.sql.functions import expr, col, broadcast, split



# In[ ]:


# Set the following to ensure no shuffling
spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true')
spark.conf.set('spark.sql.sources.v2.bucketing.pushPartValues.enabled','true')
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping','true')
spark.conf.set('spark.sql.requireAllClusterKeysForCoPartition','false')


# In[ ]:


# Joined gaming datasets table: bootcamp.gamingInfo
# Summarize game details
# Display one summary record for each game played by a gamer with total_kills, total_deaths, total_medals
# Eliminate repeating game details

spark.sql("""  
create table bootcamp.agg_gamer_games using iceberg partitioned by (bucket(16, match_id)) as

select
match_id,
playlist_id,
map_name,
player_gamertag,
player_total_kills,
player_total_deaths,
coalesce(sum(medal_count),0) as total_medals
from bootcamp.gamingInfo
group by match_id, playlist_id, map_name,player_gamertag, player_total_kills, player_total_deaths
order by player_gamertag, match_id
""")


# In[ ]:


# Q1. Which player averages the most kills per game?

spark.sql("""
select player_gamertag, avg(player_total_kills) as avg_kills_per_game
from bootcamp.agg_gamer_games
group by player_gamertag
order by avg_kills_per_game desc
limit 1
""").show()


# In[ ]:


# Q2. Which playlist gets played the most?

spark.sql("""
select playlist_id, count(*) as amt_played
from bootcamp.agg_gamer_games
group by playlist_id
order by amt_played desc
limit 1
""").show(truncate=False)


# In[ ]:


# Q3. Which map get played the most?

spark.sql("""
select map_name, count(*) as amt_played
from bootcamp.agg_gamer_games
group by map_name
order by amt_played desc
limit 1
""").show(truncate=False)


# In[ ]:


# For each game played by a gamer list the medals earned along with match_id, playlist_id, and map_name
# sum medal count
# Remove players who have not earned medals

spark.sql("""
create table bootcamp.agg_gamer_medals using iceberg partitioned by (bucket(16, match_id)) as
 
select
match_id,
playlist_id,
map_name,
player_gamertag,
medal_name,
coalesce(sum(medal_count),0) as medal_count
from bootcamp.gamingInfo
where medal_id is not null
group by match_id, playlist_id, map_name,player_gamertag, medal_name
order by player_gamertag, medal_name

""")


# In[ ]:


# Q4. Which map do players get the most Killing Spree medals on ?

spark.sql("""
select map_name, medal_name, count(*) as num_medals
from bootcamp.agg_gamer_medals
where medal_name = 'Killing Spree'
group by map_name, medal_name
order by num_medals desc
limit 1
""").show()


# In[ ]:


# Try different .sortWithinPartitions to see which has the smallest size

# Read bootcamp.gamingInfo into a dataframe

df = spark.sql(""" select * from bootcamp.gamingInfo """)


# In[ ]:


# .sortWithPartition: match_id

# Repartition and sort dataframe
df1 = df.repartition(16, (col("match_id"))).sortWithinPartitions(col("match_id"), col("playlist_id"))

# Convert the dataframe into a table
df1.write.mode("overwrite").saveAsTable("bootcamp.df1")

# Check data size
spark.sql("""
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.df1.files
""").show()


# In[ ]:


#  .sortWithPartition: mapid

# Repartition and sort dataframe
df_sort_mapid = df.repartition(16, (col("mapid"))).sortWithinPartitions(col("mapid"), col("match_id"))

# Convert the dataframe into a table
df_sort_mapid.write.mode("overwrite").saveAsTable("bootcamp.df_sort_mapid")

# Check data size
spark.sql("""
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.df_sort_mapid.files
""").show()


# In[ ]:


#  .sortWithPartition: playlist_id

# Repartition and sort dataframe
df_sort_playlist_id = df.repartition(16, (col("playlist_id"))).sortWithinPartitions(col("playlist_id"), col("match_id"))

# Convert the dataframe in
df_sort_playlist_id.write.mode("overwrite").saveAsTable("bootcamp.df_sort_playlist_id")

# Check data size
spark.sql("""
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.df_sort_playlist_id.files
""").show()


# In[ ]:


# playlist_id had the smallest data size

