# Databricks notebook source
from pyspark.sql.functions import current_timestamp,lit,to_timestamp,hour,minute,second
data = [(1,'Ram','India'),(2,'Liza','US'),(3,'Margarita','Germany'),(4,'Rita','UK'),(5,'Rita','India')]
schema = 'Id int, Name String, Country String'
df = spark.createDataFrame(data,schema)
df = df.withColumn('joining_date', current_timestamp())

df = df.withColumn('new_joingDate',lit('12.25.2002 08.10.03'))

df1 = df.withColumn('joingDateTimeStamp',to_timestamp(df.new_joingDate,'MM.dd.yyyy HH.mm.ss'))

df1 = df1.select('*',hour(df1.joingDateTimeStamp).alias('hour'),\
                    minute(df1.joingDateTimeStamp).alias('min'),\
                    second(df1.joingDateTimeStamp).alias('sec'))

df.show(truncate = False)
df1.show(truncate = False)
