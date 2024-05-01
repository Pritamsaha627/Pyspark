# Databricks notebook source
df = spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/EmpDetails.csv')
df1 = df.na.fill({'Age':0,'City':'Unknown'})
df.show()
df1.show()

# COMMAND ----------

help(df.na.fill)