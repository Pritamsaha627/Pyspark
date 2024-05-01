# Databricks notebook source
data = [(1,'Rahul',75,'Kolkata'),(2,'Raj',80,'Delhi'),(3,'Rani',94,'Kolkata'),(4,'Albert',56,'Mumbai'),(5,'Puja',67,'Chennai')]
schema = "Id int, Name string, Marks int, City string"

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import when, col
df1 = df.withColumn('Grade', when(col('Marks')>=90,'A').when(df.Marks >= 80,'B').when(col('Marks')>=70,'C').when(col('Marks')>=60,'D').otherwise('E')).orderBy('Grade')
df1.show()

# COMMAND ----------

df2 = df.select(col('*'),when(col('Marks')>=90,'A').when(df.Marks >= 80,'B').when(col('Marks')>=70,'C').when(col('Marks')>=60,'D').otherwise('E').alias("Grade_New")).orderBy("Grade_New")
df2.show()

# COMMAND ----------

help(when)