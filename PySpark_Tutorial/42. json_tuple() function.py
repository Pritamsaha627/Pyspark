# Databricks notebook source
from pyspark.sql.functions import to_json, json_tuple
data = [('Samsung',{'RAM':4,'Storage':64}),('iPhone',{'RAM':8,'Storage':128})]
schema = ['Device', 'Properties']

df = spark.createDataFrame(data,schema)
df.show(truncate=False)

df1 = df.withColumn('prop',to_json(df.Properties))
df2 = df1.select('Device',json_tuple(df1.prop,'RAM','Storage').alias('RAM','Storage'))
df1.show(truncate=False)
df2.show(truncate=False)
