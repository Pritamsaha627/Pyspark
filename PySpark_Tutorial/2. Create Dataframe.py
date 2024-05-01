# Databricks notebook source
data = [(1,'Ram',22),(2,'Arjun',45),(3,'Alia',32),(4,'Anil',66)]
schema = ['Id','Name','Roll']
df = spark.createDataFrame(data = data, schema = schema)

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

