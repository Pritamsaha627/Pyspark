# Databricks notebook source
from pyspark.sql.functions import count,min,max

data = [(1,'Ram',45,'India'),(2,'Liza',24,'US'),(3,'Margarita',31,'Germany'),(4,'Rita',47,'UK'),(5,'Rita',33,'India'),(6,'Jon',46,'US'),(7,'Margarita',14,'Germany'),(8,'Rahul',18,'UK'),(9,'Rahul',22,'India'),(10,'Margarita',35,'US')]
schema = 'Id int, Name String,Marks int, Country String'
df = spark.createDataFrame(data,schema)
df1 = df.groupBy('Country').agg(count('*'),max('Marks').alias('max_marks'),min('Marks').alias('min_marks'))
df.show()
df1.show()

# COMMAND ----------

help(df.groupBy('Country').agg)