# Databricks notebook source
data = [('Apple', 2),
('Orange', 5),
('Banana', 11),
('Mango', 17),
('Apple', 5),
('Orange', 1),
('Banana', 8),
('Mango', 19),
('Banana', 28),
('Mango', 16)]

rdd=spark.sparkContext.parallelize(data)
rdd_new = rdd.reduceByKey(lambda a,b : a+b)
print(rdd_new.collect())
rdd_sort = rdd_new.sortByKey(False)
print(rdd_sort.collect())



# COMMAND ----------

