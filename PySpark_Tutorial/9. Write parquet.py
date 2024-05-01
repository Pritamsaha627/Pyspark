# Databricks notebook source
data = [(1,'Raj'),(2,'Ani'),(3,'Bimal')]
schema = ['id','name']

df = spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

df.write.parquet(path='dbfs:/temp/parquetstore',mode='overwrite')

# COMMAND ----------

df1 = spark.read.parquet('dbfs:/temp/parquetstore')
display(df1)