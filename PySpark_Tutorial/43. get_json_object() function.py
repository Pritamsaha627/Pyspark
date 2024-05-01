# Databricks notebook source
from pyspark.sql.functions import to_json, json_tuple, get_json_object
data = [('Samsung',{'prop':{'RAM':4,'Storage':64}}),('iPhone',{'prop':{'RAM':8,'Storage':128}})]
schema = ['Device', 'Properties']

df = spark.createDataFrame(data,schema)
df.show(truncate=False)

df1 = df.withColumn('props',to_json(df.Properties))
df2 = df1.select('Device',get_json_object(df1.props,'$.prop.RAM').alias('RAM'),\
                         get_json_object(df1.props,'$.prop.Storage').alias('Storage'))
df1.show(truncate=False)
df2.show(truncate=False)


# COMMAND ----------

help(get_json_object)