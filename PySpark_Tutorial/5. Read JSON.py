# Databricks notebook source
from pyspark.sql.types import *

schema = StructType().add(field='id',data_type=IntegerType())\
                    .add(field='fruit',data_type=StringType())\
                    .add(field='color',data_type=StringType())

df = spark.read.json(path = 'dbfs:/FileStore/tables/fruit_sample1.json',schema=schema)
df.printSchema()
df.show()

# COMMAND ----------



dfm = spark.read.json(path = ['dbfs:/FileStore/tables/*.json'], multiLine=True, schema=schema)
dfm.printSchema()
dfm.show()