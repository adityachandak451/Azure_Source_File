# Databricks notebook source
dbutils.fs.ls('mnt/raw/RoadAccident/')

# COMMAND ----------

dbutils.fs.ls('mnt/raw/')

# COMMAND ----------

dbutils.fs.ls('mnt/transform')

# COMMAND ----------

#dbutils.fs.ls('mnt/transform')

PATH = "mnt/transform/"
for i in dbutils.fs.ls(PATH):
    dbutils.fs.rm(i[0],True) 

# COMMAND ----------

input_path = '/mnt/raw/RoadAccident/RoadAccident.csv'

# COMMAND ----------

df = spark.read.format("csv").load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

rdd = sc.textFile('mnt/raw/RoadAccident/RoadAccident.csv').zipWithIndex().filter(lambda a:a[1]>3).map(lambda a:a[0].split(","))
rdd.collect()

# COMMAND ----------

column_rdd = rdd.first()

column_rdd

# COMMAND ----------

main_rdd = rdd.filter(lambda a:a!=column_rdd)
Road_Accident= main_rdd.toDF(column_rdd)

# COMMAND ----------

display(Road_Accident)

# COMMAND ----------

Road_Accident.write.mode("overwrite").option("header", "true").csv("/mnt/transform/RoadAccident")



# COMMAND ----------

Road_Accident.repartition(4).write.mode("overwrite").option("header", "true").csv("/mnt/transform/RoadAccident")

# COMMAND ----------

#Road Type by Accident

from pyspark.sql.functions import *

Road_Accident.groupBy("Road_Type").agg(count('*').alias("Accident")).show()



# COMMAND ----------

df1 = Road_Accident[Road_Accident['Accident_Index'] != 'BS0000001']
display(df1)
