# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi'),(3,'Priya',56, 'Mumbai'),(3,'Murti',37, 'Indore')]
rdd = spark.sparkContext.parallelize(data)
print(type(rdd))
print(rdd.collect())

# COMMAND ----------

df = rdd.toDF(schema=['id','name','roll','city'])
print(type(df))
df.show()

# COMMAND ----------

df1 = spark.createDataFrame(rdd,schema=['id','name','roll','city'])
df1.show()