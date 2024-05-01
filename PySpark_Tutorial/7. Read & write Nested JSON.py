# Databricks notebook source
from pyspark.sql.types import *
nameschema = StructType([StructField(name='fname', dataType=StringType()),\
                        StructField(name='lname', dataType=StringType())])


schema = StructType([StructField(name='id', dataType=IntegerType()),\
                    StructField(name='Salary', dataType=IntegerType()),\
                    StructField(name='name', dataType=nameschema),\
                    StructField(name='Dept', dataType=StringType())])

df = spark.read.json(path="dbfs:/FileStore/nestedjson1.json",schema=schema,multiLine= True)
df.printSchema()
display(df)
df.show()

# COMMAND ----------

df.write.json("dbfs:/temp/nestedjson")