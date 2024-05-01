# Databricks notebook source
data = [(1,'Rahul',33),(2,'Raj',12),(3,'Priya',16),(4,'Pritam',14)]
schema = "id int, student_name string, age int"

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col('student_name').ilike('%m')).show()

# COMMAND ----------

df.filter(col('student_name').rlike('R[A-Za-z]')).show()

# COMMAND ----------

