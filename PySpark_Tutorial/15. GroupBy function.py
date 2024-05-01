# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi'), (3,'Raj',12,'Delhi'),(4,'Priya',37, 'Mumbai'),(5,'Priya',35, 'Indore'),(6,'Som',27, 'Indore')]
schema = "Id int, Name string, Age int, City string"

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# from pyspark.sql.functions import *

df1 = df.groupBy('Name').agg(max('Age').alias('MaxAge'),min('Age').alias('MinAge'))
df1.show()

# COMMAND ----------

help(df.groupBy)

# COMMAND ----------

