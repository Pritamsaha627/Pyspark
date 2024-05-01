# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi'),(3,'Priya',56, 'Mumbai')]
schema = "Id int, Name string, Roll int, City string"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df.filter((col("Id") != 2)).show()

# COMMAND ----------

