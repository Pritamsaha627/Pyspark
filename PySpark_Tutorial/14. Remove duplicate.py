# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi'), (2,'Raj',12,'Delhi'), (3,'Priya',37, 'Mumbai'),(3,'Priya',37, 'Indore')]
schema = "Id int, Name string, Roll int, City string"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.distinct().show()

# COMMAND ----------

df.dropDuplicates(['Id','Name']).show()

# COMMAND ----------

help(df.distinct())

# COMMAND ----------

help(df.dropDuplicates)

# COMMAND ----------

