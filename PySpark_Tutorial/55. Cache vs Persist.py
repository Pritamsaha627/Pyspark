# Databricks notebook source
df = spark.read.option('recursiveFileLookup','true').csv('dbfs:/FileStore/tables/',header=True).cache()
display(df)

# COMMAND ----------

import pyspark
df1 = spark.read.csv('dbfs:/FileStore/source_file/emp_nullvalues.csv',header=True).persist(pyspark.StorageLevel.DISK_ONLY)
display(df1)

# COMMAND ----------

display(df1)

# COMMAND ----------

df.unpersist()

# COMMAND ----------

display(df)

# COMMAND ----------

