# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi'),(3,'Raj',56, 'Mumbai')]
schema = "Id int, Name string, Roll int, City string"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.where(df.Id == 2).show()

# COMMAND ----------

df.where((df.Name == 'Raj') & (df.Roll == 56)).show()

# COMMAND ----------

