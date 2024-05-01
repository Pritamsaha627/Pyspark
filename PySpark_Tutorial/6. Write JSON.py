# Databricks notebook source
data = [(1,'Riya',44),(2,'Rahul',66)]
schema = "Id int,Name string,Roll int"
df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

df.write.json(path='dbfs:/FileStore/writeJsonData/student.json',mode='overwrite')

# COMMAND ----------

display(spark.read.json('dbfs:/FileStore/writeJsonData/student.json'))