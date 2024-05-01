# Databricks notebook source
data = [(1,'Ram','India'),(2,'Liza','US'),(3,'Margarita','Germany'),(4,'Rita','UK'),(5,'Rita','India'),(6,'Jon','US'),(7,'Margarita','Germany'),(8,'Rahul','UK'),(9,'Rahul','India'),(10,'Margarita','US')]
schema = 'Id int, Name String, Country String'
df = spark.createDataFrame(data,schema)
df1 = df.groupBy('Name').pivot('Country').count()
df.show()
df1.show()