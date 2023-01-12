# Databricks notebook source
# MAGIC %md
# MAGIC #### Part 1: Using Apache Spark, demonstrate how you read data from https://ride.capitalbikeshare.com/system-data.

# COMMAND ----------

bikeshareData = spark.read.option("delimiter", ",").option("header","true").option("inferSchema","true").csv("/FileStore/testCode/202212_capitalbikeshare_tripdata.csv").cache()
display(bikeshareData)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 2 - Using Apache Spark, demonstrate what would you do to the data so that, in the future, you can run efficient analytics on it?
# MAGIC 
# MAGIC Here are a few tips to optimize reading a CSV file using Apache Spark:
# MAGIC 
# MAGIC Use the `spark.sql.files.ignoreCorruptFiles` configuration property to skip corrupt or invalid files when reading.
# MAGIC 
# MAGIC Use the `spark.sql.csv.parser.lib` configuration property to specify the CSV parser library to use. The built-in library is "univocity", but "commons" and "opencsv" are also available.
# MAGIC 
# MAGIC Use the `spark.sql.csv.columnPruning` configuration property to enable column pruning, which can improve performance when reading large datasets.
# MAGIC 
# MAGIC Use the `spark.sql.csv.wholeFile` configuration property to read the entire CSV file as a single partition, which can improve performance when reading smaller files.
# MAGIC 
# MAGIC Use the `spark.sql.csv.inferSchema` configuration property to automatically infer the schema of the CSV file, which can save time and improve performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Then additonally you can perform some data cleaning transformations like:
# MAGIC 
# MAGIC `df = df.dropna() # remove null or missing values`
# MAGIC 
# MAGIC `df = df.dropDuplicates() # remove duplicate rows`
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Perform data transformation:
# MAGIC 
# MAGIC ```
# MAGIC from pyspark.sql.functions import udf
# MAGIC 
# MAGIC from pyspark.sql.types import IntegerType`
# MAGIC 
# MAGIC 
# MAGIC def clean_data(col):
# MAGIC     return col.replace("$", "").replace(",", "").cast(IntegerType())
# MAGIC     
# MAGIC 
# MAGIC udf_clean_data = udf(clean_data, IntegerType())
# MAGIC 
# MAGIC 
# MAGIC df = df.withColumn("price", udf_clean_data("price"))
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Format the data for optimal storage:
# MAGIC 
# MAGIC `df.write.format("parquet").mode("overwrite").save("path/to/formatted_data")`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Part 3: Using Apache Spark, demonstrate how you delete the latitude related columns.

# COMMAND ----------


newBikeshareDataWithoutLatLong = bikeshareData.drop("start_lat", "start_lng", "end_lat", "end_lng")
display(newBikeshareDataWithoutLatLong)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Part 4: Using Apache Spark, demonstrate how you add duration of each ride.
# MAGIC 
# MAGIC Ride Duration is calculated below in seconds

# COMMAND ----------

from pyspark.sql.functions import *

durationCalculatedInSeconds = bikeshareData.withColumn("rideDurationInSeconds", unix_timestamp(bikeshareData.ended_at)-unix_timestamp(bikeshareData.started_at))
display(durationCalculatedInSeconds)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Part 5: Using Apache Spark, demonstrate how you calculate average ride duration for each rideable type.

# COMMAND ----------

averageRideDuration = durationCalculatedInSeconds.groupBy("rideable_type").avg("rideDurationInSeconds")
display(averageRideDuration)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 6: Using Apache Spark, demonstrate how you calculate top 10 ride durations (longer than 24 hours) for each start station.

# COMMAND ----------

#24 hours = 24 * 60 * 60 seconds
topTenRidesGT24Hours = durationCalculatedInSeconds.filter(col("rideDurationInSeconds") > 60*60*24).orderBy("rideDurationInSeconds", ascending=False)
display(topTenRidesGT24Hours)

# COMMAND ----------


