# Databricks notebook source
data = [(1,'Ram','India'),(2,'Liza','US'),(3,'Margarita','Germany'),(4,'Rita','UK'),(5,'Rita','India'),(6,'Jon','US'),(7,'Margarita','Germany'),(8,'Rahul','UK'),(9,'Rahul','India'),(10,'Margarita','US')]
schema = 'Id int, Name String, Country String'
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df1 = df.collect()
print(df1)
print(df1[0])
print(df1[0][2])

# COMMAND ----------

help(df.collect)