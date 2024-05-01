# Databricks notebook source
data = [('Virat','Kohli'),('Rohit','Sharma')]
rdd = spark.sparkContext.parallelize(data)
new_rdd = rdd.map(lambda x : x +(x[0]+' '+x[1],))
print(new_rdd.collect())

# COMMAND ----------

data = [('Virat','Kohli'),('Rohit','Sharma')]
df = spark.createDataFrame(data,['fname','lname'])
rdd1 = df.rdd.map((lambda x : x +(x[0]+' '+x[1],)))
df1 = rdd1.toDF(['fname','lname','fullname'])
df1.show()