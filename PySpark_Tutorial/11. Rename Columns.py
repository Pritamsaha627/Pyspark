# Databricks notebook source
data = [(1,'Rahul',33),(2,'Raj',12),(3,'Priya',56)]
schema = "Id int, Name string, Roll int"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
# df1 = df.withColumnRenamed("Id","Id_new").withColumnRenamed("Name","Name_new").withColumnRenamed("Roll","Roll_new")
# df2 = df.selectExpr("Id as Id_new","Name as Name_new", "Roll as Roll_new")
df3 = df.select(col("Id").alias("Id_new"),col("Name").alias("Name_new"),col("Roll").alias("Roll_new"))
display(df3)
display(df)

# COMMAND ----------

help(df.selectExpr)