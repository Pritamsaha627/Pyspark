# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi')]
schema = "Id int, Name string, Roll int, City string"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.write.saveAsTable('default.employee')

# COMMAND ----------

display(spark.table('default.employee'))

# COMMAND ----------

data = [(3,'Rohan',12,'Delhi'), (4,'Priya',37, 'Mumbai')]
schema = "Id int, Name string, Roll int, City string"

df1 = spark.createDataFrame(data,schema)
display(df1)

# COMMAND ----------

df1.write.insertInto('default.employee',overwrite=False)

# COMMAND ----------

display(spark.sql('select * from default.employee'))