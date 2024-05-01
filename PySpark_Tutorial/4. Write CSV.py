# Databricks notebook source
data = [(1,'Ram','32'),(2,'Sita','44'),(3,'Liza','66')]
schema = ['id','Name','Roll']
df = spark.createDataFrame(data=data,schema=schema)


# COMMAND ----------

display(df)

# COMMAND ----------

df.write.csv(path='dbfs:/temp/student', header=True,mode='append')

# COMMAND ----------

display(spark.read.csv(path='dbfs:/temp/student', header=True))

# COMMAND ----------

