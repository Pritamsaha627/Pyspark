# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi')]
schema = ['Emp_Id', 'Emp_Name', 'Age', 'City']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col
# df1=df.withColumn("Emp_Id",df.Emp_Id.cast('int')).withColumn("Age",df.Age.cast('int'))
# df1=df.select(col('Emp_Id').cast('int'),col('Emp_Name'),col('Age').cast('int'),col('City'))
df1=df.selectExpr('cast(Emp_Id as int)','Emp_Name','cast(Age as int)','City')
df1.show()

# COMMAND ----------

