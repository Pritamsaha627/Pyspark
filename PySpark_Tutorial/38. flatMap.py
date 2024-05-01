# Databricks notebook source
data = [('Virat Kohli'),('Rohit Sharma')]
rdd = spark.sparkContext.parallelize(data)
rdd1= rdd.map(lambda x: x.split(' '))

for i in rdd1.collect():
    print(i)

# COMMAND ----------

rdd2= rdd.flatMap(lambda x: x.split(' '))

for i in rdd2.collect():
    print(i)