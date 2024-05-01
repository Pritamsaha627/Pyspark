# Databricks notebook source
data = [(1,'Ram',['English','Physics']),(2,'Liza',['Hindi','Mathematics']),(3,'Margarita',['Biology','Computer']),(4,'Rita',['Physics','Computer'])]

schema = ['Id' ,'Name', 'Subjects']

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import explode, col

df = df.withColumn('Subject',explode(col('Subjects')))
display(df)

# COMMAND ----------

data1 = [(1,'Ram','English,Physics,Hindi'),(2,'Liza','Biology,Computer,Hindi')]

schema1 = ['Id' ,'Name', 'Subjects']

df1 = spark.createDataFrame(data1,schema1)
display(df1)

# COMMAND ----------

from pyspark.sql.functions import split, col

df1=df1.withColumn('Subjects_Array',split(col('Subjects'),','))
display(df1)

# COMMAND ----------

help(split)