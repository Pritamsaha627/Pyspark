# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, MapType, IntegerType

data = [('Samsung',{'RAM':4,'Storage':64}),('iPhone',{'RAM':8,'Storage':128})]
# schema = ['Device', 'Properties']
schema = StructType([StructField('Device',StringType()),\
        StructField('Properties',MapType(StringType(),IntegerType()))])

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import explode, map_keys, map_values
df1 = df.select('Device','Properties',explode(df.Properties))
df1.show(truncate=False)

# COMMAND ----------

df2 = df.withColumn('Keys', map_keys(df.Properties)).withColumn('values', map_values(df.Properties))
df2.show(truncate = False)

# COMMAND ----------

df3 = df.withColumn('values', map_values(df.Properties))
df3.show(truncate = False)