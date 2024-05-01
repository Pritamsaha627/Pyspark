# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi'),(3,'Priya',56, 'Mumbai'),(3,'Murti',37, 'Indore')]
schema = "Id int, Name string, Roll int, City string"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df.orderBy(col("Id"),df.Roll.desc()).show()

# COMMAND ----------

