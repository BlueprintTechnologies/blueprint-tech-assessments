#!/usr/bin/env python
# coding: utf-8

# In[11]:


## Importing necessary Libraries
import findspark
findspark.init()
import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField, StringType, IntegerType , BooleanType,TimestampType, DoubleType


# In[12]:


## Creating Spark Session
spark = SparkSession.builder.getOrCreate()


# In[17]:


## reading data from source using schema

schema = StructType()       .add("ride_id",StringType(),True)       .add("rideable_type",StringType(),True)       .add("started_at", TimestampType(), True)      .add("ended_at", TimestampType(), True)      .add("start_station_name",StringType(),True)       .add("start_station_id",IntegerType(),True)       .add("end_station_name", StringType(), True)      .add("end_station_id",IntegerType(), True)      .add("start_lat",DoubleType(), True)      .add("start_lng",DoubleType(), True)      .add("end_lat",DoubleType(), True)      .add("end_lng",DoubleType(), True)      .add("member_casual",StringType(),True) 
#df = spark.read.csv('C:\\Users\\17063\\Downloads\\202209-capitalbikeshare-tripdata\\202209-capitalbikeshare-tripdata.csv',header = 'true',schema=schema)
df.show(truncate=False)
#df = spark.read.csv('https://s3.amazonaws.com/capitalbikeshare-data/index.html',header = 'true',schema=schema)

#df.limit(10).toPandas()


# In[32]:


## Dropping the latitude related columns
## Pyspark allows us to drop the required columns if we use drop function. We have read the data to dataframe and using pyspark function drop we have dropped the columns we need.
df=df.drop('start_lat','end_lat')
df.show(truncate=False) 


# In[14]:


## Calculating the duration of each ride
# Have converted the time to long to find the difference in seconds. The result will be inseconds. 
df=df.withColumn('duration',df.ended_at.cast("long")-df.started_at.cast("long"))
df.show(truncate=False)


# In[19]:


##Calculating average ride duration for each rideable type
## Used Group by on column rideabletype and used avg function to find duration for each rideableType 
df_avg_by_rideableType=df.groupBy("rideable_type").avg("duration")
df_avg_by_rideableType.show(truncate=False)
#df_average.write.mode('overwrite').format('csv').save('C:\\Users\\17063\\output')


# In[20]:


##Calculating top 10 ride durations (longer than 24 hours) for each start station
## Used windowing function and where clause to find the rides longer than 24 hours and top 10 rides for each station
df.createOrReplaceTempView('Temp')
df_ridesgreaterthan24hrs= spark.sql('select start_station_name,duration  from (select *, DENSE_RANK() OVER (PARTITION BY start_station_name ORDER BY duration DESC ) AS Rank from Temp where duration > 86400) AS Tmp WHERE Rank<=10  ')
df_ridesgreaterthan24hrs.show(truncate= False)

