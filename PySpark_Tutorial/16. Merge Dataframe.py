# Databricks notebook source
data1 = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi')]
schema1 = "Emp_Id int, Emp_Name string, Age int, City string"

df1 = spark.createDataFrame(data1,schema1)
df1.show()

# COMMAND ----------

data2 = [(2,'Raj',12,'Delhi'),(3,'Priya',56, 'Mumbai')]
schema2 = "Id int, Name string, Age int, City string"

df2 = spark.createDataFrame(data2,schema2)
df2.show()

# COMMAND ----------

df1.union(df2).distinct().show()

# COMMAND ----------

help(df1.union)