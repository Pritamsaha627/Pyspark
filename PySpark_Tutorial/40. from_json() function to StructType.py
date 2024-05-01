# Databricks notebook source
data = [('Samsung',"{'RAM':4,'Storage':64}"),('iPhone',"{'RAM':8,'Storage':128}")]
schema = ['Device', 'Properties']

df = spark.createDataFrame(data,schema)
df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([StructField('RAM',StringType()),\
                    StructField('Storage',IntegerType())])

df1 = df.withColumn('prop',from_json(df.Properties,schema))
df1.show(truncate=False)

# COMMAND ----------

df1.withColumn('RAM',df1.prop.RAM).withColumn('Storage',df1.prop.Storage).show()