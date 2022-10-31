from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType	
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import * # imported Custom Schema to read a file
from pyspark.sql import functions as f

#Create Schema Using StructType & StructField
schema = StructType([
StructField('name', StringType(), True),
StructField('birthdate', TimestampType(), True),
StructField('age', IntegerType(), True)
])

df = spark.read.csv("/Abhilash/Sample_data.tsv").option("sep","\t").schema(schema).option("header", True)
new_df = df.withColumn('birth_date',to_timestamp(col('birthdate'), "yyyyMMdd_hh:mm:ss")).drop(birthdate)

#Adding timestamp column which checks whether the given birthdate is in yyyyMMdd_hh:mm:ss or not.
df2 = new_df.withColumn("valid_time_format", f.when(f.to_timestamp(f.col("birthdate"),"yyyyMMdd_hh:mm:ss").isNotNull, False).otherwise(True))

df2.createOrReplaceTempView ("Temp_Table")
#Using Apache Spark, write the valid rows in one file. A row is considered valid if all columns within it are valid according to the rules defined in the rules file.
#dataframe which contains only valid records
Valid_records_df =  spark.sql (""" SELECT
										name, 
										birth_date,
										age
                                   FROM 
								     Temp_table
								   WHERE
								   (length(trim(name)) => 5  AND length(trim(name)) <= 100)
								   AND
								   (length(col(age)) => 5  AND length(trim(age)) <= 200)
								   AND 
								   valid_time_format = 'True' """)

#Writing valid records to one file i.e..valid_records.tsv 								   
Valid_records_df.write.csv("valid_records.tsv").option("header", "true").option("delimiter", "\t") 

#Below is the output of valid records
#Valid_records_df.show(truncate=False)
#+--------+--------------------+-----+
#|name    |          birth_date| age |
#+--------+--------------------+-----|
#|Mikee	  |   19960606_00:00:00|   26|
#|Natalie |   19830320_00:00:00|   39|
#|Nealon  |   20001116_00:00:00|   21|
#|Peter	  |   19830821_00:00:00|   39|
#|William |   19970616_00:00:00|   25|
#|Robert  |   19910817_00:00:00|   31|
#|Michael |   19900717_00:00:00|   32|
#|Ronald  |   19420228_00:00:00|   80|
#|James	  |   19911224_00:00:00|   30|
#|Elizabe |   19600121_00:00:00|   62|
#|Debra	  |   20001108_00:00:00|   21|
#|Daniel  |   19840821_00:00:00|   38|
#|Grogory |   19940505_00:00:00|   28|
#|Heather |   19560629_00:00:00|   66|
#+--------+--------------------+-----+

#Using Apache Spark, write the invalid data in another file. For the invalid data, in a dedicated column, store information about all the fields which were invalid
#dataframe which contains only Invalid records and adding dedicated column with reason for invalid record
Invalid_records_df =  spark.sql (""" SELECT 
											name, 
											birth_date, 
											age,
										CASE WHEN
											(length(trim(name)) < 5  AND length(trim(name)) > 100)
										THEN
											"Name doesn't meet the specified length requirement"
										WHEN
											valid_time_format = 'False'
										THEN 
											"Date provided is not in proper format"
										WHEN 
											(length(col(age)) < 5  AND length(trim(age)) > 200)
										THEN
											"Age doesn't meet the specified length requirement"
										END AS "Invalid_Record_Reason"
                                    FROM 
										Temp_table
								   WHERE
									 (length(trim(name)) < 5  AND length(trim(name)) > 100)
								   AND
								     (length(col(age)) < 5  AND length(trim(age)) > 200)
								   AND 
								     valid_time_format = 'False' """)

#Writing Invalid records to one file i.e..valid_records.tsv 								   
Invalid_records_df.write.csv("Invalid_records.tsv").option("header", "true").option("delimiter", "\t")

#Below is the output of Invalid records
#Invalid_records_df.show(truncate=False)
#+--------+--------------------+-----+--------------------------------------------------------+
#|name    |          birth_date| age |                               Invalid_Record_Reason    |
#+--------+--------------------+-----|--------------------------------------------------------+
#|Kate	  |   19930105_00:00:00|   29|   Name doesn't meet the specified length requirement   |
#|Paul    |   19960713_00:00:00|   26|   Name doesn't meet the specified length requirement   |
#|John	  |   19760627_00:00:00|   46|   Name doesn't meet the specified length requirement   |
#|Lisa	  |   19560608_00:00:00|   66|   Name doesn't meet the specified length requirement   |
#|Mary    |   19620330_00:00:00|   60|   Name doesn't meet the specified length requirement   |
#+--------+--------------------+-----+--------------------------------------------------------+
