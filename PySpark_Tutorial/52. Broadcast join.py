# Databricks notebook source
df1 = spark.read.csv('dbfs:/FileStore/source_file/races.csv',header= True,inferSchema=True)
display(df1)

# COMMAND ----------

data = [(1,'London'),(2,'Paris'),(3,'Honkong',),(4,'California'),(5,'Delhi'),(6,'Mumbai'),(7,'Kolkata'),(8,'Budapest'),(9,'Amsterdam'),(10,'Chennai')]
sch = 'raceId int, place string'
df2 = spark.createDataFrame(data,sch)
display(df2)

# COMMAND ----------

df1.join(df2,df1.raceId == df2.raceId,'left').explain()

# COMMAND ----------

from pyspark.sql.functions import broadcast
df1.join(df2,df1.raceId == df2.raceId).explain()

# COMMAND ----------

