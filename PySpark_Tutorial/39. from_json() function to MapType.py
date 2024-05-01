# Databricks notebook source
data = [('Samsung',"{'RAM':4,'Storage':64}"),('iPhone',"{'RAM':8,'Storage':128}")]
schema = ['Device', 'Properties']

df = spark.createDataFrame(data,schema)
df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType, IntegerType

df1 = df.withColumn('prop',from_json(df.Properties,MapType(StringType(),IntegerType())))
df1.show(truncate=False)

# COMMAND ----------

df2 = df1.withColumn('RAM',df1.prop.RAM)\
        .withColumn('Storage',df1.prop.Storage)
df2.show(truncate=False)