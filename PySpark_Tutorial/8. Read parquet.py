# Databricks notebook source
df = spark.read.parquet('dbfs:/FileStore/*.parquet')
df.printSchema()
display(df)
print(df.count())

# COMMAND ----------

