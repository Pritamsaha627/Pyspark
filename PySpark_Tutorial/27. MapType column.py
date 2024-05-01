# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, MapType, IntegerType

data = [('Samsung',{'RAM':4,'Storage':64}),('iPhone',{'RAM':8,'Storage':128})]
# schema = ['Device', 'Properties']
schema = StructType([StructField('Device',StringType()),\
        StructField('Properties',MapType(StringType(),IntegerType()))])

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------



# COMMAND ----------

df1 = df.withColumn('stg',df.Properties['Storage']) 
display(df1)

# COMMAND ----------

help(MapType)

# COMMAND ----------

