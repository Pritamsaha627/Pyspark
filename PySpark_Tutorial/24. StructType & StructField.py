# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

data = [(1,('Ram','Saha'),30),(2,('Liza','Roy'),32),(3,('Margarita','Desuza'),44),(4,('Rita','Ray'),66)]

nameschema= StructType([StructField(name='Fname', dataType=StringType()),\
                    StructField(name='Lname', dataType=StringType())])

schema = StructType([StructField(name='Id', dataType=IntegerType()),\
                    StructField(name='Name', dataType=nameschema),\
                    StructField(name='Roll', dataType=IntegerType())])

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

