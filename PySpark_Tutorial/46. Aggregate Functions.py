# Databricks notebook source
# MAGIC %md
# MAGIC approx_count_distinct(),collect_list(),collect_set(),countDistinct()

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, collect_list, collect_set, countDistinct
data = [(1,'Ram',25000),(2,'Liza',30000),(3,'Margarita',25000),(4,'Rita',35000),(5,'Rita',30000)]
schema = 'Id int, Name String, Salary int'
df = spark.createDataFrame(data,schema)
df.show()
df.select(approx_count_distinct('Salary')).show()
df.select(collect_list('Salary')).show(truncate = False)
df.select(collect_set('Salary')).show(truncate = False)
df.select(countDistinct('Salary')).show(truncate = False)

# COMMAND ----------

