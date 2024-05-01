# Databricks notebook source
from pyspark.sql.functions import lit, col



data = [(1,'Rahul',33),(2,'Raj',12),(3,'Priya',56)]
schema = "Id int, Name string, Roll int"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df = df.withColumn("City",lit("Kolkata"))
display(df)

# COMMAND ----------

df = df.withColumn("Age", col("Roll")+5)
display(df)

# COMMAND ----------

df = df.withColumn("Country", lit("India")).withColumn("Fees", col('Age')*100)
display(df)
