# Databricks notebook source
dbutils.fs.mkdirs('dbfs:/FileStore/tables/emp/2023/')

# COMMAND ----------

from pyspark.sql.functions import input_file_name
df = spark.read.option('recursiveFileLookup','true').csv('dbfs:/FileStore/tables/',header=True)
df = df.withColumn('location',input_file_name())
display(df)

# COMMAND ----------

help(dbutils.fs)

# COMMAND ----------

