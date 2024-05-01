# Databricks notebook source
def multi_fun(x,y):
    return x*y

# COMMAND ----------

spark.udf.register('multi',multi_fun)

# COMMAND ----------

for i in spark.catalog.listFunctions():
    if(i.name == 'multi'):
        print(i)