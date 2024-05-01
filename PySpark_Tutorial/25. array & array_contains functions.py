# Databricks notebook source
data = [(1,'Ram','Saha',30),(2,'Liza','Ray',32),(3,'Margarita','Desuza',44),(4,'Rita','Ray',66)]

schema = "Id int,fName string, lName string, Age int"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import array, col, array_contains

df1 = df.withColumn('Name',array(col('fName'),col('lName')))
display(df1)

# COMMAND ----------

df2 = df1.withColumn('value',array_contains(col('Name'),'Ray'))
display(df2)

# COMMAND ----------

help(array)

# COMMAND ----------

help(array_contains)

# COMMAND ----------

