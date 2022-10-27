from pyspark.sql.functions import *
from pyspark import SparkFiles
import zipfile
import pandas as pd

#Part 1: Using Apache Spark, demonstrate how you read data from https://ride.capitalbikeshare.com/system-data.

url = "https://s3.amazonaws.com/capitalbikeshare-data/index.html/download/?format=csv"

sc.addFile(url)
 
path  = SparkFiles.get('download')
# read the dataset using the compression zip
pdf = pd.read_csv(r "file://" + path, header=True,compression='zip',inferSchema=True)

# creating spark session and coverting pandas dataframe to spark datafram
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("zip reader").getOrCreate()
sparkDF=spark.createDataFrame(pdf)

#Part 3: Using Apache Spark, demonstrate how you delete the latitude related columns.
drop_df  = sparkDF.drop("start_lat","end_lat")
drop_df.printSchema()

#Part 4: Using Apache Spark, demonstrate how you add duration of each ride.
duration_df = col("ended_at").cast("long") - col("started_at").cast("long")
total_duration_df = sparkDF.withColumn( "duration_of_ride", duration_df.cast("timestamp")).groupBy("ride_id")
total_duration_df.show(truncate=False)

#Part 5: Using Apache Spark, demonstrate how you calculate average ride duration for each rideable type.
total_duration_df.createOrReplaceTempView("capitalbikeShare_temp")
avg_dur_df = spark.sql("Select rideable_type, AVG(duration_of_ride) AS avg_duration FROM capitalbikeShare_temp GROUP BY rideable_type")
avg_dur_df.show(truncate=False)

#Part 6: Using Apache Spark, demonstrate how you calculate top 10 ride durations (longer than 24 hours) for each start station.

DiffInHours_df = total_duration_df.withColumn("DiffInHours",round(col("duration_of_ride")/3600))
DiffInHours_df.createOrReplaceTempView("capitalbikeShare_temp1")
top_ten_df =  spark.sql("""Select * from 
				  (Select *, dense_rank() over (partition by start_station_name,start_station_id 
				   order by start_station_id) as rank from capitalbikeShare_temp1 where DiffInHours > '24.0')
				  where rank <= 10 """)
top_ten_df.show(truncate=False)				  

#Part 2: Using Apache Spark, demonstrate what would you do to the data so that, in the future, you can run efficient analytics on it?
#Use Serialized data format’s prefer writing an intermediate file in Serialized and optimized formats like Avro,Parquet.Since any transformations on these formats performs better than text, CSV, and JSON.

df.write.parquet(“/output/capitalbikeshare.parquet”)