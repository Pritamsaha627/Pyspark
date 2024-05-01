# Databricks notebook source
data1 = [(1,'Rahul',33),(2,'Raj',12),(3,'Priya',16)]
schema1 = "Id int, Name string, Age int"

df1 = spark.createDataFrame(data1,schema1)
df1.show()

# COMMAND ----------

data2 = [(1,'Rahul','Kolkata'),(2,'Raj','Delhi'),(4,'Nil','UP')]
schema2 = "Id int, Name string, City string"

df2 = spark.createDataFrame(data2,schema2)
df2.show()

# COMMAND ----------

from pyspark.sql.functions import col

df1.alias("dat1").join(df2.alias("dat2"),col('dat1.Id') == col('dat2.Id'),"right").select(col('dat1.Id'),col('dat1.Name'),col('Age'),col('City')).show()

# COMMAND ----------

help(df1.join)

# COMMAND ----------

