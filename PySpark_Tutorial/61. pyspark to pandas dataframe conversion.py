# Databricks notebook source
data = [(1,'Rahul',33),(2,'Raj',12),(3,'Priya',16)]
schema = "Id int, Name string, Age int"

df = spark.createDataFrame(data,schema)
df.show()
print(type(df))

# COMMAND ----------

df_panda = df.toPandas()
print(df_panda)
print(type(df_panda))

# COMMAND ----------

